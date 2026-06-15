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

    el = round(time.time() - started, 1)
    print("[finviz-universe] %d tickers | short_float=%d float=%d rel_volume=%d | %ss"
          % (n, n_short, n_float, n_relvol, el))
    return {"statusCode": 200,
            "body": json.dumps({"n_tickers": n, "short_float": n_short, "float": n_float,
                                "rel_volume": n_relvol, "elapsed_s": el})}
