"""
justhodl-ticker-360 — Cross-engine enrichment producer.

Builds the pre-computed "full picture" index: for every ticker covered by
2+ data domains, the confluence summary plus each domain's per-ticker slice.

Reads (via shared ticker_360 hub): all SOURCES domains.
Writes: data/ticker-360.json — {generated_at, universe_size, tickers: {...}}

Any engine or frontend page reads ONE key for the composed picture instead
of hand-wiring N sources. The hub (ticker_360.enrich) remains available for
on-demand per-ticker views.

Fail-soft: a dead domain is marked unavailable, never fails the run.
"""
from __future__ import annotations

import json
import os
import time
from datetime import datetime, timezone

import boto3

import ticker_360

BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
OUT_KEY = "data/ticker-360.json"
VERSION = "1.0"

s3 = boto3.client("s3", "us-east-1")

MIN_COVERAGE = 2  # only index tickers seen by 2+ domains ("full picture" names)


def _compact_td(td):
    """Trim a ticker_data slice to a compact, JSON-safe summary."""
    if td is None:
        return None
    if not isinstance(td, dict):
        return {"value": str(td)[:200]}
    out = {}
    for k, v in td.items():
        if k.startswith("_"):
            continue
        if isinstance(v, (str, int, float, bool)) or v is None:
            out[k] = v
        elif isinstance(v, (list, tuple)) and len(v) <= 8:
            out[k] = [str(x)[:120] if not isinstance(x, (str, int, float, bool)) else x
                      for x in v]
        elif isinstance(v, dict) and len(v) <= 12:
            out[k] = {kk: (vv if isinstance(vv, (str, int, float, bool)) or vv is None
                           else str(vv)[:120]) for kk, vv in v.items()}
        if len(out) >= 25:
            break
    return out


def lambda_handler(event, context):
    t0 = time.time()
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    cache: dict = {}
    tickers = ticker_360.universe(s3, cache=cache)
    indexed = {}
    for t in tickers:
        try:
            view = ticker_360.enrich(t, s3, cache=cache)
        except Exception:  # noqa: BLE001
            continue
        conf = view.get("confluence", {})
        if conf.get("coverage_count", 0) < MIN_COVERAGE:
            continue
        doms = {}
        for d, dv in view.get("domains", {}).items():
            if not dv.get("available"):
                continue
            td = dv.get("ticker_data")
            if td is None and not dv.get("ticker_covered"):
                continue
            doms[d] = {"as_of": dv.get("as_of"),
                       "data": _compact_td(td)}
        indexed[t] = {"coverage_count": conf.get("coverage_count"),
                      "coverage_pct": conf.get("coverage_pct"),
                      "domains": doms}
    payload = {"contract": "ticker-360.v1", "version": VERSION,
               "generated_at": now,
               "universe_size": len(tickers),
               "indexed_tickers": len(indexed),
               "min_coverage": MIN_COVERAGE,
               "domains": sorted(ticker_360.SOURCES),
               "tickers": indexed,
               "elapsed_s": round(time.time() - t0, 1)}
    body = json.dumps(payload, separators=(",", ":"), ensure_ascii=False,
                      allow_nan=False, default=str)
    s3.put_object(Bucket=BUCKET, Key=OUT_KEY, Body=body.encode(),
                  ContentType="application/json",
                  CacheControl="max-age=300")
    return {"ok": True, "out": OUT_KEY, "universe": len(tickers),
            "indexed": len(indexed), "bytes": len(body)}
