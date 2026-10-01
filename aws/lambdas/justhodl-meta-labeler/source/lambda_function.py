"""Publish unavailable meta-labeler authority until causal inputs are proven.

The former writer fitted on unqualified graded rows and emitted TAKE/SKIP.
Retained predecessor bytes and offline reproductions are in tests/fixtures/
meta-labeler. A publication timestamp is not historical data availability.
No provider, graded-history, signal-table or secret read is needed to withdraw
these claims. Existing deployment and scheduling controls remain unchanged.
"""
import json
import time
from datetime import datetime, timezone
import boto3
from meta_labeler_authority import blocked_packet

S3 = boto3.client('s3', region_name='us-east-1')
BUCKET = 'justhodl-dashboard-live'
OUT_KEY = 'data/meta-labeler.json'
VERSION = '1.1.0'


def lambda_handler(event=None, context=None):
    t0 = time.time()
    out = blocked_packet()
    out.update(version=VERSION, generated_at=datetime.now(timezone.utc).isoformat(),
               duration_s=round(time.time() - t0, 1),
               diagnostics=['Historical metrics and recommendations withheld; no model fitted.'])
    S3.put_object(Bucket=BUCKET, Key=OUT_KEY,
                  Body=json.dumps(out, allow_nan=False).encode('utf-8'),
                  ContentType='application/json', CacheControl='public, max-age=1800')
    return {'statusCode': 200, 'body': json.dumps({
        'status': out['status'], 'qualification_status': out['qualification_status'],
        'uplift_pp': None})}
