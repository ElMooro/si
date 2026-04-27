"""
scrapeMacroData — DEPRECATED stub

Background
----------
This Lambda was the original OpenBB-era macro data pipeline that scraped
FRED + ECB + OECD + NYFED + Treasury and wrote to s3://macro-data-lake/.
That bucket is no longer canonical (justhodl-dashboard-live is) and the
data flow has been replaced by:

  justhodl-daily-report-v3   — every 5 min, writes data/report.json
  justhodl-fred-proxy        — FRED series fetch (cached)
  justhodl-ecb-proxy         — ECB data
  justhodl-treasury-proxy    — Treasury auctions/yields

The old Lambda has been failing 100% (21/21 errored in last 7d) because
the scrape targets and bucket layout no longer match.

This stub returns a structured 200 documenting the deprecation so any
remaining EB rule or invoker stops generating CloudWatch errors.

To finish the deprecation:
  1. Delete EB rule (see config.eventbridge_rules) — needs AWS Console / CLI
  2. Delete Lambda function — needs AWS Console / CLI
  3. Delete this directory from the repo
"""
import json
from datetime import datetime, timezone


def lambda_handler(event, context):
    return {
        'statusCode': 200,
        'headers': {
            'Content-Type': 'application/json',
            'Access-Control-Allow-Origin': '*',
        },
        'body': json.dumps({
            'service':     'scrapeMacroData',
            'status':      'deprecated',
            'replaced_by': [
                'justhodl-daily-report-v3',
                'justhodl-fred-proxy',
                'justhodl-ecb-proxy',
                'justhodl-treasury-proxy',
            ],
            'note':        'OpenBB-era macro pipeline. EB rule + Lambda should be deleted.',
            'invoked_at':  datetime.now(timezone.utc).isoformat(timespec='seconds'),
            'request_id':  getattr(context, 'aws_request_id', None),
        }),
    }
