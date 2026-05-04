"""
justhodl-momentum-scanner — Universe-wide momentum factor scanner.

For each S&P 500 ticker (from screener/data.json):
  - 1m, 3m, 6m, 12m total returns
  - 12-1 momentum (12m return excluding last month — academic factor)
  - vol-adjusted momentum (return / 60d realized vol)
  - max drawdown over 252d
  - distance from 252d high / above 252d low
  - acceleration (1m_ann - 6m_ann): is momentum accelerating?
  - composite momentum score (multi-factor blend, percentile-ranked)

Output: data/momentum-scanner.json
Schedule: daily 12:30 UTC weekdays
"""
import json
import os
import time
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone, timedelta
from collections import defaultdict
import math
import boto3

S3 = boto3.client("s3", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"
KEY = "data/momentum-scanner.json"

POLYGON_KEY = os.environ.get("POLYGON_KEY", "zvEY_KYYMHoAN0JqY7n2Ze6q0kBuJX_d")


def polygon_bars(ticker, days=300):
    end = datetime.now(timezone.utc).date().isoformat()
    start = (datetime.now(timezone.utc).date() - timedelta(days=days + 100)).isoformat()
    url = (
        f"https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/day/{start}/{end}"
        f"?adjusted=true&sort=asc&limit=5000&apiKey={POLYGON_KEY}"
    )
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "justhodl-mom/1.0"})
        with urllib.request.urlopen(req, timeout=15) as r:
            data = json.loads(r.read().decode())
        if data.get("results"):
            return [(b["t"], b["c"], b["h"], b["l"], b.get("v", 0)) for b in data["results"]]
    except Exception as e:
        print(f"[poly] {ticker} failed: {e}")
    return []


def returns_at_offsets(bars):
    if len(bars) < 250:
        return None
    last = bars[-1][1]
    out = {}
    for w in (21, 63, 126, 252):
        if len(bars) > w:
            prev = bars[-1 - w][1]
            if prev > 0:
                out[f"ret_{w}d"] = (last / prev - 1) * 100
    if len(bars) > 252:
        try:
            close_252_ago = bars[-1 - 252][1]
            close_21_ago = bars[-1 - 21][1]
            if close_252_ago > 0:
                out["mom_12_1"] = (close_21_ago / close_252_ago - 1) * 100
        except Exception:
            pass
    return out


def realized_vol_60d(bars):
    if len(bars) < 61:
        return None
    closes = [b[1] for b in bars[-61:]]
    log_rets = []
    for i in range(1, len(closes)):
        if closes[i-1] > 0 and closes[i] > 0:
            log_rets.append(math.log(closes[i] / closes[i-1]))
    if len(log_rets) < 30:
        return None
    mean = sum(log_rets) / len(log_rets)
    var = sum((r - mean) ** 2 for r in log_rets) / len(log_rets)
    sd = math.sqrt(var)
    return sd * math.sqrt(252) * 100


def max_drawdown(bars, window=252):
    if len(bars) < window:
        return None
    closes = [b[1] for b in bars[-window:]]
    peak = closes[0]
    max_dd = 0
    for c in closes:
        if c > peak:
            peak = c
        if peak > 0:
            dd = (c / peak - 1) * 100
            if dd < max_dd:
                max_dd = dd
    return max_dd


def distance_from_extremes(bars, window=252):
    if len(bars) < window:
        return None, None
    closes = [b[1] for b in bars[-window:]]
    highs = [b[2] for b in bars[-window:]]
    lows = [b[3] for b in bars[-window:]]
    last = closes[-1]
    high = max(highs)
    low = min(lows)
    pct_below_high = (last / high - 1) * 100 if high > 0 else None
    pct_above_low = (last / low - 1) * 100 if low > 0 else None
    return pct_below_high, pct_above_low


def percentile_rank(values, target):
    if not values:
        return None
    n = len(values)
    below = sum(1 for v in values if v < target)
    return (below / n) * 100


def lambda_handler(event=None, context=None):
    started = time.time()

    try:
        obj = S3.get_object(Bucket=BUCKET, Key="screener/data.json")
        screener = json.loads(obj["Body"].read())
        stocks = screener.get("stocks") or screener.get("results") or []
    except Exception as e:
        return {"statusCode": 500, "body": f"screener load failed: {e}"}

    meta_by_ticker = {s["symbol"]: {
        "name": s.get("name"),
        "sector": s.get("sector"),
        "industry": s.get("industry"),
        "marketCap": s.get("marketCap"),
    } for s in stocks if "symbol" in s}

    tickers = list(meta_by_ticker.keys())
    print(f"[mom-scan] universe size: {len(tickers)}")

    bars_by_ticker = {}
    with ThreadPoolExecutor(max_workers=15) as ex:
        futs = {ex.submit(polygon_bars, t, 300): t for t in tickers}
        done = 0
        for f in as_completed(futs):
            t = futs[f]
            bars = f.result()
            done += 1
            if bars and len(bars) >= 250:
                bars_by_ticker[t] = bars
            if done % 100 == 0:
                print(f"[mom-scan] fetched {done}/{len(tickers)}")
    print(f"[mom-scan] {len(bars_by_ticker)}/{len(tickers)} with sufficient data")

    rows = []
    for t, bars in bars_by_ticker.items():
        rets = returns_at_offsets(bars)
        if not rets:
            continue
        vol = realized_vol_60d(bars)
        dd = max_drawdown(bars, 252)
        below_high, above_low = distance_from_extremes(bars, 252)
        vol_adj_mom = (rets.get("ret_63d", 0) / vol) if vol and vol > 0 else None
        ret_21 = rets.get("ret_21d", 0)
        ret_126 = rets.get("ret_126d", 0)
        accel = None
        if ret_21 is not None and ret_126 is not None:
            ann_short = ret_21 * 12
            ann_long = ret_126 * 2
            accel = ann_short - ann_long

        meta = meta_by_ticker.get(t, {})
        rows.append({
            "ticker": t,
            "name": meta.get("name"),
            "sector": meta.get("sector"),
            "industry": meta.get("industry"),
            "last_close": bars[-1][1],
            "ret_1m": round(rets.get("ret_21d", 0), 2),
            "ret_3m": round(rets.get("ret_63d", 0), 2),
            "ret_6m": round(rets.get("ret_126d", 0), 2),
            "ret_12m": round(rets.get("ret_252d", 0), 2),
            "mom_12_1": round(rets.get("mom_12_1", 0), 2) if rets.get("mom_12_1") is not None else None,
            "vol_60d": round(vol, 2) if vol is not None else None,
            "vol_adj_mom_3m": round(vol_adj_mom, 3) if vol_adj_mom is not None else None,
            "max_drawdown_252d": round(dd, 2) if dd is not None else None,
            "pct_below_252d_high": round(below_high, 2) if below_high is not None else None,
            "pct_above_252d_low": round(above_low, 2) if above_low is not None else None,
            "acceleration": round(accel, 2) if accel is not None else None,
        })

    def field_pctile(field):
        vals = [r[field] for r in rows if r.get(field) is not None]
        return {r["ticker"]: percentile_rank(vals, r[field]) for r in rows if r.get(field) is not None}

    pct_3m = field_pctile("ret_3m")
    pct_12m = field_pctile("ret_12m")
    pct_12_1 = field_pctile("mom_12_1")
    pct_voladj = field_pctile("vol_adj_mom_3m")

    for r in rows:
        t = r["ticker"]
        scores = []
        for d in (pct_3m, pct_12m, pct_12_1, pct_voladj):
            v = d.get(t)
            if v is not None:
                scores.append(v)
        r["composite_score"] = round(sum(scores) / len(scores), 2) if scores else None

    composite_sorted = sorted([r for r in rows if r.get("composite_score") is not None],
                              key=lambda x: -x["composite_score"])
    mom_12_1_sorted = sorted([r for r in rows if r.get("mom_12_1") is not None],
                             key=lambda x: -x["mom_12_1"])
    accel_sorted = sorted([r for r in rows if r.get("acceleration") is not None],
                          key=lambda x: -x["acceleration"])
    voladj_sorted = sorted([r for r in rows if r.get("vol_adj_mom_3m") is not None],
                           key=lambda x: -x["vol_adj_mom_3m"])
    near_high_sorted = sorted(rows, key=lambda x: -(x.get("pct_below_252d_high") or -1e9))
    near_low_sorted = sorted([r for r in rows if r.get("pct_above_252d_low") is not None],
                             key=lambda x: x["pct_above_252d_low"])

    by_sector = defaultdict(list)
    for r in rows:
        if r.get("sector") and r.get("composite_score") is not None:
            by_sector[r["sector"]].append(r)

    sector_summary = {}
    for sec, recs in by_sector.items():
        recs_sorted = sorted(recs, key=lambda x: -x["composite_score"])
        avg = sum(r["composite_score"] for r in recs_sorted) / len(recs_sorted)
        sector_summary[sec] = {
            "n": len(recs_sorted),
            "avg_composite": round(avg, 2),
            "top_5": [{"ticker": r["ticker"], "score": r["composite_score"], "ret_3m": r["ret_3m"]} for r in recs_sorted[:5]],
            "bottom_5": [{"ticker": r["ticker"], "score": r["composite_score"], "ret_3m": r["ret_3m"]} for r in recs_sorted[-5:]],
        }

    sector_ranked = dict(sorted(sector_summary.items(), key=lambda x: -x[1]["avg_composite"]))

    out = {
        "version": "1.0",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "duration_s": round(time.time() - started, 2),
        "summary": {
            "n_universe": len(tickers),
            "n_with_data": len(rows),
            "top_composite": composite_sorted[0]["ticker"] if composite_sorted else None,
            "top_composite_score": composite_sorted[0]["composite_score"] if composite_sorted else None,
            "best_sector": list(sector_ranked.keys())[0] if sector_ranked else None,
            "worst_sector": list(sector_ranked.keys())[-1] if sector_ranked else None,
        },
        "rankings": {
            "composite_top_50": composite_sorted[:50],
            "mom_12_1_top_20": mom_12_1_sorted[:20],
            "accelerating_top_20": accel_sorted[:20],
            "vol_adjusted_top_20": voladj_sorted[:20],
            "near_52w_high_top_20": [r for r in near_high_sorted if r.get("pct_below_252d_high", -100) > -3][:20],
            "bottom_50_composite": composite_sorted[-50:],
        },
        "by_sector": sector_ranked,
    }

    body = json.dumps(out, default=str).encode("utf-8")
    S3.put_object(Bucket=BUCKET, Key=KEY, Body=body, ContentType="application/json", CacheControl="public, max-age=3600")
    print(f"[mom-scan] wrote {len(body):,}b in {out['duration_s']}s top={out['summary']['top_composite']}")

    return {
        "statusCode": 200,
        "body": json.dumps({
            "n_universe": len(tickers),
            "n_with_data": len(rows),
            "top_composite": out["summary"]["top_composite"],
            "top_composite_score": out["summary"]["top_composite_score"],
            "best_sector": out["summary"]["best_sector"],
            "duration_s": out["duration_s"],
        }),
    }


if __name__ == "__main__":
    print(json.dumps(lambda_handler({}, None), indent=2, default=str))
