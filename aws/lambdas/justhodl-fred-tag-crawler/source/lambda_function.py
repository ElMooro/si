"""justhodl-fred-tag-crawler — E4 v1 (ops 4452).

Materialize the top-5k FRED catalog (E4's 'top 5k' clause) by popularity:
5 x 1000-row pages of series/search ordered by popularity -> id, title,
frequency, units, seasonal adj, popularity, last_updated ->
data/warm/fred-catalog.json.gz + summary. This is the census that upgrades
E11's fred_feeds_in_use PROXY into a real numerator. Full ~800k enumeration
= E10 backfill territory, stated not silent. F4 snapshot per page."""
import gzip
import json
import os
import time
import urllib.request
from datetime import datetime, timezone

import boto3

BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
KEY = os.environ.get("FRED_API_KEY", "2f057499936072679d8843d7fce99989")
s3 = boto3.client("s3", region_name="us-east-1")
try:
    from raw_snapshot import snapshot
except Exception:
    snapshot = None


def lambda_handler(event, context):
    now = datetime.now(timezone.utc)
    rows, raw_keys, errs = [], [], []
    for page in range(5):
        # ops 4453: series/search returned 0 for 'the' (stopword-filtered
        # despite 5 clean pages) — the tags endpoint is the enumeration
        # API. tag_names=usa is the broadest tag; popularity-ordered.
        url = ("https://api.stlouisfed.org/fred/tags/series"
               f"?tag_names=usa&api_key={KEY}&file_type=json"
               "&order_by=popularity&sort_order=desc"
               f"&limit=1000&offset={page * 1000}")
        try:
            req = urllib.request.Request(url, headers={
                "User-Agent": "justhodl/1.0"})
            with urllib.request.urlopen(req, timeout=45) as r:
                raw = r.read()
            if snapshot:
                raw_keys.append(snapshot("fred", url, raw))
            for s in (json.loads(raw).get("seriess") or []):
                rows.append({"id": s.get("id"), "title": s.get("title"),
                             "frequency": s.get("frequency_short"),
                             "units": s.get("units_short"),
                             "sa": s.get("seasonal_adjustment_short"),
                             "popularity": s.get("popularity"),
                             "last_updated": s.get("last_updated")})
            time.sleep(0.6)
        except Exception as e:
            errs.append(f"page{page}: {type(e).__name__}: {str(e)[:60]}")
    seen, uniq = set(), []
    for r in rows:
        if r["id"] and r["id"] not in seen:
            seen.add(r["id"])
            uniq.append(r)
    s3.put_object(Bucket=BUCKET, Key="data/warm/fred-catalog.json.gz",
                  Body=gzip.compress(json.dumps(
                      {"as_of": now.isoformat(timespec="seconds"),
                       "method": "tags/series tag_names=usa by popularity, "
                                 "5x1000 pages; "
                                 "full 800k census = E10",
                       "raw_snapshot_keys": raw_keys,
                       "n_series": len(uniq),
                       "series": uniq}).encode()),
                  ContentType="application/gzip")
    s3.put_object(Bucket=BUCKET,
                  Key="data/warm/fred-catalog-summary.json",
                  Body=json.dumps({
                      "as_of": now.isoformat(timespec="seconds"),
                      "n_series": len(uniq), "pages_ok": 5 - len(errs),
                      "errors": errs,
                      "top10": [r["id"] for r in uniq[:10]]}).encode(),
                  ContentType="application/json", CacheControl="no-cache")
    res = {"ok": True, "n_series": len(uniq), "errors": errs,
           "top5": [r["id"] for r in uniq[:5]]}
    print(json.dumps(res))
    return {"statusCode": 200, "body": json.dumps(res)}
