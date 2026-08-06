"""justhodl-cftc-full-datasets — E6 v1 (ops 4449).

Nightly deep COT from the official CFTC Socrata API
(publicreporting.cftc.gov): legacy futures-only (6dca-aqww) and
disaggregated futures-only (72hh-3qpy), newest 5,000 report-rows each
(~2y across all markets) -> data/warm/cftc/{dataset}.json.gz +
latest-summary. F4 snapshots; explicit failures; deeper backfill = E10."""
import gzip
import json
import os
import urllib.request
from datetime import datetime, timezone

import boto3

BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
s3 = boto3.client("s3", region_name="us-east-1")
try:
    from raw_snapshot import snapshot
except Exception:
    snapshot = None

BASE = "https://publicreporting.cftc.gov/resource/"
DATASETS = {
    "legacy_futures": {
        "id": "6dca-aqww",
        "fields": ["market_and_exchange_names", "report_date_as_yyyy_mm_dd",
                   "open_interest_all", "noncomm_positions_long_all",
                   "noncomm_positions_short_all",
                   "comm_positions_long_all", "comm_positions_short_all"]},
    "disaggregated_futures": {
        "id": "72hh-3qpy",
        "fields": ["market_and_exchange_names", "report_date_as_yyyy_mm_dd",
                   "open_interest_all", "m_money_positions_long_all",
                   "m_money_positions_short_all",
                   "prod_merc_positions_long_all",
                   "prod_merc_positions_short_all"]},
}


def lambda_handler(event, context):
    now = datetime.now(timezone.utc)
    summary = {"as_of": now.isoformat(timespec="seconds"), "datasets": {}}
    for name, cfg in DATASETS.items():
        url = (f"{BASE}{cfg['id']}.json?$select="
               + ",".join(cfg["fields"])
               + "&$order=report_date_as_yyyy_mm_dd%20DESC&$limit=5000")
        try:
            req = urllib.request.Request(url, headers={
                "User-Agent": "JustHodl research admin@justhodl.ai"})
            with urllib.request.urlopen(req, timeout=60) as r:
                raw = r.read()
            raw_key = snapshot("cftc", url, raw) if snapshot else None
            rows = json.loads(raw)
            dates = sorted({x.get("report_date_as_yyyy_mm_dd", "")[:10]
                            for x in rows if x})
            s3.put_object(
                Bucket=BUCKET, Key=f"data/warm/cftc/{name}.json.gz",
                Body=gzip.compress(json.dumps(
                    {"dataset": name, "socrata_id": cfg["id"],
                     "source_url": url, "raw_snapshot_key": raw_key,
                     "n_rows": len(rows),
                     "span_note": "newest 5000 rows; deep backfill = E10",
                     "rows": rows}).encode()),
                ContentType="application/gzip")
            summary["datasets"][name] = {
                "n_rows": len(rows),
                "latest_report": dates[-1] if dates else None,
                "oldest_in_pull": dates[0] if dates else None,
                "n_report_dates": len(dates)}
        except Exception as e:
            summary["datasets"][name] = {
                "data_unavailable": True,
                "reason": f"{type(e).__name__}: {str(e)[:80]}"}
    s3.put_object(Bucket=BUCKET, Key="data/warm/cftc/latest-summary.json",
                  Body=json.dumps(summary).encode(),
                  ContentType="application/json", CacheControl="no-cache")
    res = {"ok": True,
           "loaded": {k: v.get("n_rows") for k, v in
                      summary["datasets"].items() if v.get("n_rows")},
           "failed": [k for k, v in summary["datasets"].items()
                      if v.get("data_unavailable")]}
    print(json.dumps(res))
    return {"statusCode": 200, "body": json.dumps(res)}
