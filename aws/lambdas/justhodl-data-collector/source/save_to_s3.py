"""
justhodl-data-collector — archives latest data/report.json snapshot

Schedule: rate(1 hour) via EB rule 'justhodl-hourly-collection'

Background
----------
Original source called `urllib.request.urlopen('https://api.justhodl.ai/')`
which since Phase 1 lockdown (2026-04-22) returns 401 — the AI chat
Worker requires the x-justhodl-token header.

The intent of "data-collector" is to archive system snapshots into
historical S3 over time. Reading the live report from
justhodl-dashboard-live/data/report.json (produced every 5 min by
justhodl-daily-report-v3 per memory) and copying it timestamped into
the historical bucket is exactly that.

Output
------
  s3://justhodl-historical-data-1758485495/data/YYYY/MM/DD/HH-MM-SS.json
"""
import json
import os
import boto3
from datetime import datetime, timezone

s3 = boto3.client("s3")

LIVE_BUCKET = os.environ.get("LIVE_BUCKET", "justhodl-dashboard-live")
LIVE_KEY = os.environ.get("LIVE_KEY", "data/report.json")
ARCHIVE_BUCKET = os.environ.get("ARCHIVE_BUCKET", "justhodl-historical-data-1758485495")


def lambda_handler(event, context):
    now = datetime.now(timezone.utc)
    archive_key = f"data/{now.strftime('%Y/%m/%d/%H-%M-%S')}.json"

    try:
        # Use server-side copy when possible — avoids reading the body into Lambda memory
        s3.copy_object(
            Bucket=ARCHIVE_BUCKET,
            Key=archive_key,
            CopySource={"Bucket": LIVE_BUCKET, "Key": LIVE_KEY},
            MetadataDirective="REPLACE",
            Metadata={
                "archived_at": now.isoformat(timespec="seconds"),
                "source_bucket": LIVE_BUCKET,
                "source_key": LIVE_KEY,
            },
        )
    except s3.exceptions.NoSuchKey:
        return {
            "statusCode": 404,
            "body": json.dumps({
                "ok": False,
                "error": f"source missing: s3://{LIVE_BUCKET}/{LIVE_KEY}",
            }),
        }
    except Exception as e:
        # Don't swallow — let Lambda error out so CloudWatch counts it
        print(f"::DATA_COLLECTOR_ERROR:: {type(e).__name__}: {e}")
        raise

    return {
        "statusCode": 200,
        "body": json.dumps({
            "ok": True,
            "archived_to": f"s3://{ARCHIVE_BUCKET}/{archive_key}",
            "source": f"s3://{LIVE_BUCKET}/{LIVE_KEY}",
        }),
    }
