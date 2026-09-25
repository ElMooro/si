"""Native original-source liquidity research; scheduled publication only."""
import json
import os
import boto3
from liquidity_pulse_store import run
from liquidity_pulse_model import CONTRACT, CURRENT

S3_BUCKET = os.environ.get('S3_BUCKET', 'justhodl-dashboard-live')


def lambda_handler(event, context):
    result = run(boto3.client('s3'), S3_BUCKET)
    response = {'engine_contract': CONTRACT, 'packet_key': CURRENT, **result}
    return {'statusCode': 200, 'headers': {'Content-Type': 'application/json'},
            'body': json.dumps(response, allow_nan=False)}
