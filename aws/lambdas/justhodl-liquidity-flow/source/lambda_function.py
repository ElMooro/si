"""Native original-source liquidity research; scheduled publication only."""
import json
import os
import boto3
from liquidity_flow_store import run

S3_BUCKET = os.environ.get('S3_BUCKET', 'justhodl-dashboard-live')


def lambda_handler(event, context):
    result = run(boto3.client('s3'), S3_BUCKET)
    return {'statusCode': 200, 'headers': {'Content-Type': 'application/json'},
            'body': json.dumps(result, allow_nan=False)}
