"""justhodl-feed-registry v1.0 — the fleet's data-freshness ledger (ops 3415, #10).

Lists every data/*.json in the bucket with its age; flags stale feeds
(default SLA 48h; weekly feeds get 8d via name hints). The sentinel diffs
this registry and alerts on NEWLY-stale feeds — so a dead upstream can no
longer silently poison downstream joins for weeks.
Feed: data/feed-registry.json
"""
import json
import os
import time
from datetime import datetime, timezone

import boto3

VERSION = "1.3.0"
S3_BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
OUT_KEY = "data/feed-registry.json"
WEEKLY_HINTS = ("13f", "weekly", "cot", "clone", "playbook", "wl-", "brain",
                "tv-notes", "monthly")
s3 = boto3.client("s3", "us-east-1")


def lambda_handler(event, context):
    t0 = time.time()
    now = datetime.now(timezone.utc)
    try:
        retired = set(json.loads(s3.get_object(Bucket=S3_BUCKET,
            Key="config/feed-retired.json")["Body"].read()).get("retired") or [])
    except Exception:
        retired = set()
    rows, token = [], None
    while True:
        kw = {"Bucket": S3_BUCKET, "Prefix": "data/", "MaxKeys": 1000}
        if token:
            kw["ContinuationToken"] = token
        r = s3.list_objects_v2(**kw)
        for o in r.get("Contents", []):
            k = o["Key"]
            if not k.endswith(".json"):
                continue
            if k.count("/") > 1:   # top-level feeds only — no subdir archives
                continue
            age_h = round((now - o["LastModified"]).total_seconds() / 3600, 1)
            sla = 24 * 8 if any(h in k for h in WEEKLY_HINTS) else 48
            rows.append({"key": k, "age_h": age_h, "sla_h": sla,
                         "size": o["Size"],
                         "retired": k in retired,
                         "stale": (age_h > sla) and k not in retired})
        token = r.get("NextContinuationToken")
        if not token:
            break
    rows.sort(key=lambda x: -x["age_h"])
    stale = [x for x in rows if x["stale"]]
    out = {"ok": True, "version": VERSION,
           "generated_at": now.isoformat(),
           "elapsed_s": round(time.time() - t0, 2),
           "n_feeds": len(rows), "n_stale": len(stale),
           "stale": stale[:60], "feeds": rows}
    s3.put_object(Bucket=S3_BUCKET, Key=OUT_KEY,
                  Body=json.dumps(out, separators=(",", ":")).encode(),
                  ContentType="application/json", CacheControl="max-age=300")
    print(f"[feed-registry] {len(rows)} feeds, {len(stale)} stale, "
          f"{round(time.time() - t0, 1)}s")
    return {"statusCode": 200, "body": json.dumps(
        {"ok": True, "n": len(rows), "stale": len(stale)})}
