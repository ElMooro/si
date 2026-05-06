#!/usr/bin/env python3
"""Step 258 — Force history-snapshotter to build audit index immediately.

The snapshotter only builds the index when current minute < 5 (~once
per hour). To populate /audit.html on first deploy, this script
temporarily overrides time.now via a sync invoke at the top of the
hour — except we can't actually do that. Easier: invoke the function
directly with an event payload that bypasses the minute-check.

Simplest path: call the index builder by importing the module from
the deployed Lambda is not possible. So we instead do two things:
  1. Sync invoke once (regular run; index won't build because minute>5)
  2. Read DDB ourselves and write the index directly from this ops
     script (we have the same boto3 access).

That way audit.html has data within a minute of deploy.
"""
import json
import os
import time
from datetime import datetime, timezone

import boto3

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
DDB_TABLE = "justhodl-history"
INDEX_KEY = "data/history-index.json"
LAMBDA_NAME = "justhodl-history-snapshotter"
REPORT_PATH = "aws/ops/reports/258_audit_index_seed.json"


def main():
    ddb = boto3.client("dynamodb", region_name=REGION)
    s3 = boto3.client("s3", region_name=REGION)
    lam = boto3.client("lambda", region_name=REGION)

    started = time.time()

    # Step A: invoke snapshotter once to keep DDB fresh
    try:
        resp = lam.invoke(FunctionName=LAMBDA_NAME, InvocationType="RequestResponse", Payload=b"{}")
        invoke_status = resp.get("StatusCode")
        invoke_err = resp.get("FunctionError")
        print(f"[258] snapshotter invoke: status={invoke_status} err={invoke_err}")
    except Exception as e:
        print(f"[258] invoke err: {e}")
        invoke_status = None
        invoke_err = str(e)

    # Step B: build the audit index directly from DDB (mirror the
    # logic in justhodl-history-snapshotter._build_history_index)
    feeds_meta = {}
    last_key = None
    pages = 0
    while True:
        kw = {"TableName": DDB_TABLE,
              "ProjectionExpression": "pk, sk, content_hash"}
        if last_key:
            kw["ExclusiveStartKey"] = last_key
        resp = ddb.scan(**kw)
        for item in resp.get("Items", []):
            pk = item.get("pk", {}).get("S")
            sk = item.get("sk", {}).get("S")
            ch = item.get("content_hash", {}).get("S")
            if not pk or not sk:
                continue
            if pk not in feeds_meta:
                feeds_meta[pk] = {
                    "snapshot_count": 0, "first_ts": sk, "last_ts": sk,
                    "latest_hash": ch, "recent_timestamps": [],
                }
            m = feeds_meta[pk]
            m["snapshot_count"] += 1
            if sk < m["first_ts"]:
                m["first_ts"] = sk
            if sk > m["last_ts"]:
                m["last_ts"] = sk
                m["latest_hash"] = ch
            m["recent_timestamps"].append(sk)
        last_key = resp.get("LastEvaluatedKey")
        pages += 1
        if not last_key or pages > 30:
            break

    feeds_out = []
    for pk, m in sorted(feeds_meta.items()):
        m["recent_timestamps"].sort(reverse=True)
        m["recent_timestamps"] = m["recent_timestamps"][:50]
        feed_key = pk.split("#", 1)[1] if "#" in pk else pk
        feeds_out.append({
            "feed_key": feed_key,
            "pk": pk,
            "snapshot_count": m["snapshot_count"],
            "first_seen": m["first_ts"],
            "last_seen": m["last_ts"],
            "latest_hash": m["latest_hash"],
            "recent_timestamps": m["recent_timestamps"],
        })
    feeds_out.sort(key=lambda f: -f["snapshot_count"])

    index_doc = {
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "ddb_table": DDB_TABLE,
        "n_feeds": len(feeds_out),
        "n_snapshots_total": sum(f["snapshot_count"] for f in feeds_out),
        "ddb_pages_scanned": pages,
        "duration_s": round(time.time() - started, 2),
        "seeded_by": "step_258",
        "feeds": feeds_out,
    }
    s3.put_object(
        Bucket=BUCKET, Key=INDEX_KEY,
        Body=json.dumps(index_doc, default=str).encode(),
        ContentType="application/json",
        CacheControl="public, max-age=600",
    )
    print(f"[258] wrote {INDEX_KEY}: {len(feeds_out)} feeds, "
          f"{index_doc['n_snapshots_total']} snapshots")

    out = {
        "lambda_invoke_status": invoke_status,
        "lambda_invoke_err": invoke_err,
        "index": {
            "n_feeds": index_doc["n_feeds"],
            "n_snapshots_total": index_doc["n_snapshots_total"],
            "ddb_pages_scanned": pages,
            "duration_s": index_doc["duration_s"],
        },
        "top_5_feeds_by_snapshot_count": [
            {"feed_key": f["feed_key"], "count": f["snapshot_count"],
             "last_seen": f["last_seen"]}
            for f in feeds_out[:5]
        ],
    }
    os.makedirs(os.path.dirname(REPORT_PATH), exist_ok=True)
    with open(REPORT_PATH, "w") as f:
        json.dump(out, f, indent=2, default=str)
    print(json.dumps(out, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
