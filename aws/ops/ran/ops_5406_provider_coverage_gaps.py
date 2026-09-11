"""ops_5406 -- Polygon + Finviz coverage vs warehouse.

Does not call api.polygon.io or Finviz (no secrets in this script).
Lists S3 prefixes we already paid to store, then writes
data/provider-coverage-gaps.json: IN_WAREHOUSE vs PLAN_SURFACE_NOT_SEEN.
"""
from __future__ import annotations

import json
sys_exit = None
import sys
from datetime import datetime, timezone
from pathlib import Path

import boto3

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from ops_report import report  # noqa: E402

B = "justhodl-dashboard-live"
REGION = "us-east-1"

POLY_PREFIXES = [
    "data/warm/polygon-full/",
    "data/warm/us-equities-daily/",
    "data/warm/polygon/",
    "data/polygon-related-graph.json",
    "data/finviz-universe.json",
    "data/finviz-signals.json",
]

# Product surfaces on Stocks Advanced / Elite Finviz -- inventory only.
POLYGON_SURFACE = [
    ("grouped_daily_aggs", ["data/warm/polygon-full/grouped/", "data/warm/us-equities-daily/"]),
    ("ticker_reference", ["data/symbol-dictionary.json", "data/symbol-map.json"]),
    ("related_tickers", ["data/polygon-related-graph.json"]),
    ("snapshots", ["data/warm/polygon-full/snapshot/", "data/warm/polygon/snapshot/"]),
    ("minute_aggs", ["data/warm/polygon-full/minute/", "data/warm/polygon/minute/"]),
    ("trades", ["data/warm/polygon-full/trades/"]),
    ("quotes_nbbo", ["data/warm/polygon-full/quotes/"]),
    ("options", ["data/warm/polygon-full/options/", "data/warm/options/"]),
    ("fundamentals_ratios", ["data/warm/polygon-full/financials/", "data/warm/polygon/financials/"]),
    ("news", ["data/warm/polygon-full/news/"]),
    ("technicals", ["data/warm/polygon-full/technicals/"]),
    ("forex", ["data/warm/polygon-full/fx/"]),
    ("crypto", ["data/warm/polygon-full/crypto/"]),
    ("indices", ["data/warm/polygon-full/indices/"]),
]
FINVIZ_SURFACE = [
    ("screener_universe", ["data/finviz-universe.json"]),
    ("signals_export", ["data/finviz-signals.json"]),
    ("insider", ["data/warm/finviz/insider/", "data/finviz-insider.json"]),
    ("news", ["data/warm/finviz/news/", "data/finviz-news.json"]),
    ("options_skew", ["data/warm/finviz/options/"]),
    ("maps", ["data/warm/finviz/maps/"]),
    ("earnings", ["data/warm/finviz/earnings/"]),
]


def _exists_prefix(s3, prefix):
    if not prefix.endswith("/") and not prefix.endswith(".json"):
        prefix = prefix
    if prefix.endswith(".json"):
        try:
            s3.head_object(Bucket=B, Key=prefix)
            return True, prefix, 1
        except Exception:
            return False, prefix, 0
    resp = s3.list_objects_v2(Bucket=B, Prefix=prefix, Delimiter="/", MaxKeys=5)
    n = len(resp.get("Contents") or []) + len(resp.get("CommonPrefixes") or [])
    return n > 0, prefix, n


def _score(s3, surface):
    rows = []
    for name, prefixes in surface:
        hits = []
        for p in prefixes:
            ok, key, n = _exists_prefix(s3, p)
            if ok:
                hits.append({"key": key, "listed": n})
        rows.append({
            "surface": name,
            "in_warehouse": bool(hits),
            "hits": hits,
            "looked_for": prefixes,
        })
    return rows


def main():
    with report("ops_5406_provider_coverage_gaps") as R:
        R.heading("ops 5406 -- Polygon/Finviz coverage gaps")
        s3 = boto3.client("s3", region_name=REGION)
        poly = _score(s3, POLYGON_SURFACE)
        fv = _score(s3, FINVIZ_SURFACE)
        for row in poly + fv:
            (R.ok if row["in_warehouse"] else R.warn)(
                "%s %s" % (row["surface"], "IN" if row["in_warehouse"] else "GAP"))
        payload = {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "schema_version": 1,
            "source": "ops_5406",
            "rule": "warehouse prefix check only -- no Polygon/Finviz HTTP",
            "polygon": poly,
            "finviz": fv,
            "polygon_gaps": [r["surface"] for r in poly if not r["in_warehouse"]],
            "finviz_gaps": [r["surface"] for r in fv if not r["in_warehouse"]],
            "already_pulled": [
                "Polygon grouped daily aggs under data/warm/polygon-full/grouped and us-equities-daily",
                "Finviz universe + signals hot JSON",
                "Related-ticker graph",
            ],
            "do_not": "Stand up a new crawler fleet from this op. Expand justhodl-polygon-daily / finviz-universe only after this map.",
        }
        s3.put_object(
            Bucket=B,
            Key="data/provider-coverage-gaps.json",
            Body=json.dumps(payload, default=str).encode("utf-8"),
            ContentType="application/json",
        )
        back = json.loads(s3.get_object(Bucket=B, Key="data/provider-coverage-gaps.json")["Body"].read())
        if back.get("schema_version") != 1:
            R.fail("read-back")
            sys.exit(1)
        R.ok("GREEN -- data/provider-coverage-gaps.json gaps_p=%s gaps_f=%s" % (
            payload["polygon_gaps"], payload["finviz_gaps"]))


if __name__ == "__main__":
    main()
