"""justhodl-polygon-daily-snapshot — E2 (ops 4460, APPROVED as APR-0001 by
Khalid). Nightly grouped-daily for ALL US stocks (one Polygon call) ->
data/warm/us-equities-daily/{date}.json.gz + summary. Walks back up to 4
days to find the last trading session; empty market days stated, never
zero-filled. F4 snapshot of raw bytes."""
import gzip
import json
import os
import urllib.request
from datetime import datetime, timedelta, timezone

import boto3

BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
s3 = boto3.client("s3", region_name="us-east-1")
try:
    from raw_snapshot import snapshot
except Exception:
    snapshot = None


def _key():
    k = os.environ.get("POLYGON_API_KEY")
    if k:
        return k
    try:
        ssm = boto3.client("ssm", region_name="us-east-1")
        return ssm.get_parameter(Name="/justhodl/polygon/api-key",
                                 WithDecryption=True)["Parameter"]["Value"]
    except Exception:
        return None


def lambda_handler(event, context):
    now = datetime.now(timezone.utc)
    key = _key()
    if not key:
        out = {"ok": False, "data_unavailable": True,
               "reason": "no POLYGON_API_KEY in env or SSM"}
        print(json.dumps(out))
        return {"statusCode": 200, "body": json.dumps(out)}
    for back in range(1, 5):
        d = (now - timedelta(days=back)).strftime("%Y-%m-%d")
        url = ("https://api.polygon.io/v2/aggs/grouped/locale/us/market/"
               f"stocks/{d}?adjusted=true&apiKey={key}")
        try:
            with urllib.request.urlopen(urllib.request.Request(
                    url, headers={"User-Agent": "justhodl/1.0"}),
                    timeout=60) as r:
                raw = r.read()
        except Exception as e:
            print(f"{d}: {type(e).__name__}: {str(e)[:60]}")
            continue
        data = json.loads(raw)
        rows = data.get("results") or []
        if not rows:
            continue
        raw_key = (snapshot("polygon", url.split("apiKey=")[0] + "apiKey=***",
                            raw) if snapshot else None)
        s3.put_object(Bucket=BUCKET,
                      Key=f"data/warm/us-equities-daily/{d}.json.gz",
                      Body=gzip.compress(json.dumps(
                          {"date": d, "n_tickers": len(rows),
                           "raw_snapshot_key": raw_key,
                           "results": rows}).encode()),
                      ContentType="application/gzip")
        s3.put_object(Bucket=BUCKET,
                      Key="data/warm/us-equities-daily/latest-summary.json",
                      Body=json.dumps({
                          "as_of": now.isoformat(timespec="seconds"),
                          "session": d, "n_tickers": len(rows),
                          "approved_as": "APR-0001 (Khalid)",
                          "sample": rows[:3]}).encode(),
                      ContentType="application/json",
                      CacheControl="no-cache")
        res = {"ok": True, "session": d, "n_tickers": len(rows)}
        print(json.dumps(res))
        return {"statusCode": 200, "body": json.dumps(res)}
    res = {"ok": False, "data_unavailable": True,
           "reason": "no trading session with results in last 4 days"}
    print(json.dumps(res))
    return {"statusCode": 200, "body": json.dumps(res)}
