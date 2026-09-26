"""Scheduled complete research inventory; public HTTP never runs acquisition."""
import json, os
import boto3
from botocore.config import Config
import board_store as store

def lambda_handler(event=None, context=None):
    event = event if isinstance(event, dict) else {}
    if event.get('validate_only') is True:
        store.qualified()
        return {'statusCode': 200, 'body': json.dumps({'contract': store.candidate.CONTRACT, 'validated': True, 'published': False})}
    if event.get('httpMethod') or (event.get('requestContext') or {}).get('http'):
        return {'statusCode': 307, 'headers': {'Location': 'https://justhodl.ai/'+store.CURRENT+'?exact=1&nogen=1', 'Cache-Control': 'no-store'}, 'body': ''}
    if not getattr(context, 'aws_request_id', None): raise ValueError('Native AWS context required')
    client = boto3.client('s3', region_name='us-east-1', config=Config(connect_timeout=4, read_timeout=20, retries={'max_attempts': 2}))
    result = store.run(client, os.environ.get('S3_BUCKET', 'justhodl-dashboard-live'), store.slot(event))
    return {'statusCode': 200, 'body': json.dumps({'contract': store.candidate.CONTRACT, **result}, allow_nan=False)}
