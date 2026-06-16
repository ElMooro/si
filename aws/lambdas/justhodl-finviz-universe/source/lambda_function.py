"""justhodl-finviz-universe — pull the whole US equity universe from Finviz Elite export
across 5 views (overview/ownership/technical/performance/valuation), merge by ticker, and
publish:
  data/finviz-universe.json  — full by_ticker record (~11.3k tickers, ~40 fields)
  data/finviz-short.json     — slim short-float index for cheap squeeze/flow consumption

This is the canonical whole-market snapshot: one authenticated call per view replaces
thousands of per-ticker FMP fetches and fills gaps FMP can't (short float, float, rel-vol).
"""
import json
import time
from datetime import datetime, timezone
import boto3
import finviz as FV

s3 = boto3.client("s3", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"


def lambda_handler(event=None, context=None):
    started = time.time()
    uni = FV.build_universe()
    n = len(uni)
    n_short = sum(1 for r in uni.values() if r.get("short_float_pct") is not None)
    n_relvol = sum(1 for r in uni.values() if r.get("rel_volume") is not None)
    n_float = sum(1 for r in uni.values() if r.get("float_shares") is not None)
    now = datetime.now(timezone.utc).isoformat()

    s3.put_object(
        Bucket=BUCKET, Key="data/finviz-universe.json",
        Body=json.dumps({"generated_at": now, "source": "finviz-elite-export",
                         "n_tickers": n, "n_with_short_float": n_short, "by_ticker": uni},
                        separators=(",", ":"), default=str).encode(),
        ContentType="application/json", CacheControl="public, max-age=900")

    slim = {tk: {k: r.get(k) for k in ("short_float_pct", "short_ratio", "float_shares", "rel_volume", "avg_volume")
                 if r.get(k) is not None}
            for tk, r in uni.items() if r.get("short_float_pct") is not None}
    s3.put_object(
        Bucket=BUCKET, Key="data/finviz-short.json",
        Body=json.dumps({"generated_at": now, "n": len(slim), "by_ticker": slim},
                        separators=(",", ":"), default=str).encode(),
        ContentType="application/json", CacheControl="public, max-age=900")

    # ── sector heatmap aggregation (whole-market perf by sector) ──
    from collections import defaultdict
    secs = defaultdict(list)
    for r in uni.values():
        if r.get("sector") and r.get("perf_m") is not None:
            secs[r["sector"]].append(r)
    def _capw(rows, key):  # market-cap-weighted perf = the sector's actual move (not micro-cap-skewed mean)
        num = den = 0.0
        for x in rows:
            v = x.get(key); mc = x.get("market_cap")
            if v is not None and mc:
                num += v * mc; den += mc
        return round(num / den, 2) if den else None
    def _med(a):
        a = sorted(a); return round(a[len(a)//2], 2) if a else None
    heat = []
    for sec, rows in secs.items():
        rs = sorted(rows, key=lambda x: x.get("perf_m") or 0)
        heat.append({
            "sector": sec, "n": len(rows),
            "avg_perf_w": _capw(rows, "perf_w"),
            "avg_perf_m": _capw(rows, "perf_m"),
            "avg_perf_ytd": _capw(rows, "perf_ytd"),
            "median_perf_m": _med([x["perf_m"] for x in rows if x.get("perf_m") is not None]),
            "total_mktcap_b": round(sum((x.get("market_cap") or 0) for x in rows) / 1000, 1),
            "top": [{"ticker": x["ticker"], "perf_m": x.get("perf_m"), "mktcap_m": x.get("market_cap")} for x in rs[-6:][::-1]],
            "bottom": [{"ticker": x["ticker"], "perf_m": x.get("perf_m")} for x in rs[:6]],
        })
    heat.sort(key=lambda x: x["avg_perf_m"] if x["avg_perf_m"] is not None else -999, reverse=True)
    s3.put_object(Bucket=BUCKET, Key="data/finviz-heatmap.json",
                  Body=json.dumps({"generated_at": now, "n_sectors": len(heat), "sectors": heat},
                                  separators=(",", ":"), default=str).encode(),
                  ContentType="application/json", CacheControl="public, max-age=900")

    el = round(time.time() - started, 1)
    print("[finviz-universe] %d tickers | short_float=%d float=%d rel_volume=%d | %ss"
          % (n, n_short, n_float, n_relvol, el))
    return {"statusCode": 200,
            "body": json.dumps({"n_tickers": n, "short_float": n_short, "float": n_float,
                                "rel_volume": n_relvol, "elapsed_s": el})}
