"""One bounded dimensional Treasury history acquisition per invocation.

The old page-number/date-only cursor is retained at its old key for inspection,
but cannot grant completion to this v2 backfill. Re-fetch the oldest boundary
date so a page split cannot strand rows from the same observation period.
"""
import json
import os
from datetime import date, timedelta
import boto3
from raw_snapshot import snapshot  # Declares the transitive capture deploy dependency.
from treasury_fiscal_model import CONTRACT, DATASETS, encoded
from treasury_fiscal_store import acquire, merge, summary, get, now, error_code

BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
s3 = boto3.client("s3", region_name="us-east-1")
PROG_KEY = "data/audit/treasury-fiscal-backfill-v2.json"


def lambda_handler(event, context):
    progress, etag, _ = get(s3, BUCKET, PROG_KEY)
    progress = progress or {"contract": CONTRACT, "tasks": {}, "cursor": 0}
    tasks = progress["tasks"]
    requested = (event or {}).get("dataset")
    if requested is not None and requested not in DATASETS:
        return {"statusCode": 400, "body": json.dumps({"ok": False, "error": "unknown dataset"})}
    choices = list(DATASETS)
    choices = choices[progress["cursor"]:] + choices[:progress["cursor"]]
    if requested:
        choices = [requested]
    dataset = next((ds for ds in choices if tasks.get(ds, {}).get("status") != "complete"), None)
    if not dataset:
        return {"statusCode": 200, "body": json.dumps({"ok": True, "status": "complete"})}
    try:
        current, _, _ = get(s3, BUCKET, "data/warm/treasury/" + dataset + ".json.gz")
        if not current or current.get("contract") != CONTRACT or not current.get("records"):
            raise ValueError("current dimensional acquisition must run first")
        boundary = min(item["row"]["record_date"] for item in current["records"])
        # lt next day means include the boundary date, completing split periods.
        before = (date.fromisoformat(boundary) + timedelta(days=1)).isoformat()
        page = acquire(dataset, BUCKET, before=before)
        rows = page["document"]["data"]
        total = int(page["document"].get("meta", {}).get("total-count", -1))
        complete = total == len(rows)
        if rows and not complete and min(r["record_date"] for r in rows) >= boundary:
            raise ValueError("page cannot progress past boundary; increase reviewed acquisition bound")
        merged = merge(s3, BUCKET, dataset, [page])
        summary(s3, BUCKET)
        tasks[dataset] = {"status": "complete" if complete else "running", "updated_at": now(),
                          "oldest_observation": min(r["row"]["record_date"] for r in merged["records"]),
                          "records_retained": merged["n_records"], "query_rows": len(rows),
                          "query_total": total, "replay": merged["replay"],
                          "completion_scope": "current provider vintage; no historical release-time claim"}
        progress["cursor"] = (list(DATASETS).index(dataset) + 1) % len(DATASETS)
        progress["generated_at"] = now()
        try:
            s3.put_object(Bucket=BUCKET, Key=PROG_KEY, Body=encoded(progress), ContentType="application/json",
                          **({"IfMatch": etag} if etag else {"IfNoneMatch": "*"}))
        except Exception as exc:
            if error_code(exc) not in ("412", "PreconditionFailed", "409", "ConditionalRequestConflict"):
                raise
            # History already committed safely. A concurrent cursor wins; replay
            # the harmless acquisition next time rather than lose its progress.
            return {"statusCode": 200, "body": json.dumps({"ok": True, "progress_conflict": True})}
        return {"statusCode": 200, "body": json.dumps({"ok": True, "dataset": dataset, **tasks[dataset]})}
    except Exception as exc:
        return {"statusCode": 503, "body": json.dumps({"ok": False, "dataset": dataset, "error": type(exc).__name__})}
