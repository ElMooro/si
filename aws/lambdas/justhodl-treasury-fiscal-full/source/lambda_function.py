"""justhodl-treasury-fiscal-full — E7 v1 (ops 4448).

Nightly deep history from the official fiscaldata.treasury.gov API — the
fiscal side of the liquidity stack: total public debt (Debt to the Penny)
and the TGA daily operating cash balance, ~10y each ->
data/warm/treasury/{dataset}.json.gz + latest-summary. F4 snapshots;
failures explicit; pagination bounded and honest about span fetched."""
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

BASE = "https://api.fiscaldata.treasury.gov/services/api/fiscal_service"
DATASETS = {
    "debt_to_penny": {
        "path": "/v2/accounting/od/debt_to_penny",
        "params": "?sort=-record_date&page[size]=2500",
        "fields": {"date": "record_date",
                   "value": "tot_pub_debt_out_amt"},
        "unit": "USD"},
    "tga_operating_cash": {
        "path": "/v1/accounting/dts/operating_cash_balance",
        "params": ("?filter=account_type:eq:Treasury General Account "
                   "(TGA) Closing Balance"
                   "&sort=-record_date&page[size]=2500"),
        "fields": {"date": "record_date",
                   "value": "open_today_bal"},
        "unit": "USD millions"},
}


def lambda_handler(event, context):
    now = datetime.now(timezone.utc)
    summary = {"as_of": now.isoformat(timespec="seconds"), "datasets": {}}
    for name, cfg in DATASETS.items():
        url = BASE + cfg["path"] + cfg["params"].replace(" ", "%20")
        try:
            req = urllib.request.Request(url, headers={
                "User-Agent": "JustHodl research admin@justhodl.ai"})
            with urllib.request.urlopen(req, timeout=60) as r:
                raw = r.read()
            raw_key = snapshot("treasury", url, raw) if snapshot else None
            data = (json.loads(raw).get("data") or [])
            fd, fv = cfg["fields"]["date"], cfg["fields"]["value"]
            rows = []
            for o in data:
                try:
                    rows.append({"date": o.get(fd),
                                 "value": float(o.get(fv))})
                except (TypeError, ValueError):
                    continue
            rows.sort(key=lambda x: x["date"])
            s3.put_object(
                Bucket=BUCKET, Key=f"data/warm/treasury/{name}.json.gz",
                Body=gzip.compress(json.dumps(
                    {"dataset": name, "unit": cfg["unit"],
                     "source_url": url, "raw_snapshot_key": raw_key,
                     "n_obs": len(rows),
                     "span_note": "single-page pull (2500 newest); "
                                  "older backfill = E10 orchestrator",
                     "observations": rows}).encode()),
                ContentType="application/gzip")
            cur = rows[-1] if rows else {}
            summary["datasets"][name] = {
                "current": cur.get("value"), "date": cur.get("date"),
                "unit": cfg["unit"], "n_obs": len(rows),
                "span": (f"{rows[0]['date']}..{rows[-1]['date']}"
                         if rows else None)}
        except Exception as e:
            summary["datasets"][name] = {
                "data_unavailable": True,
                "reason": f"{type(e).__name__}: {str(e)[:80]}"}
    s3.put_object(Bucket=BUCKET,
                  Key="data/warm/treasury/latest-summary.json",
                  Body=json.dumps(summary).encode(),
                  ContentType="application/json", CacheControl="no-cache")
    res = {"ok": True,
           "loaded": {k: v.get("n_obs") for k, v in
                      summary["datasets"].items() if v.get("n_obs")},
           "failed": [k for k, v in summary["datasets"].items()
                      if v.get("data_unavailable")]}
    print(json.dumps(res))
    return {"statusCode": 200, "body": json.dumps(res)}
