"""Recorded parent research composition; no provider API or notifications."""
import json
import boto3
from botocore.config import Config
from massive_research_model import CONTRACT, CURRENT, permissions, strict
from massive_research_store import run, bounded, missing
PUBLISHED_KEY = 'data/massive-research.json'


def publish_current(client, bucket, key, raw, condition):
    if key != PUBLISHED_KEY or set(condition) not in ({'IfMatch'}, {'IfNoneMatch'}):
        raise ValueError('Reviewed composite target and conditional write required')
    client.put_object(Bucket=bucket, Key=PUBLISHED_KEY, Body=raw, ContentType='application/json', CacheControl='no-store', **condition)


def lambda_handler(event=None, context=None):
    event = event if isinstance(event, dict) else {}
    if event.get('validate_only') is True:
        return {'statusCode': 200, 'body': json.dumps({'validation_only': True, 'contract': CONTRACT, 'published': False})}
    client = boto3.client('s3', region_name='us-east-1', config=Config(connect_timeout=5, read_timeout=20,
        retries={'max_attempts': 2}, tcp_keepalive=True)); bucket = 'justhodl-dashboard-live'
    if (event.get('requestContext') or {}).get('http') or event.get('httpMethod') or event.get('action') == 'current_state':
        try: packet = strict(bounded(client.get_object(Bucket=bucket, Key=CURRENT)['Body']))
        except Exception as exc:
            if not missing(exc): raise
            packet = {}
        if packet.get('contract') != CONTRACT:
            return {'statusCode': 503, 'body': json.dumps({'reason': 'native_composite_research_unavailable'})}
        permissions(packet)
        return {'statusCode': 200, 'headers': {'Content-Type': 'application/json', 'Cache-Control': 'no-store'}, 'body': json.dumps(packet)}
    execution_id = getattr(context, 'aws_request_id', None)
    if not execution_id: raise ValueError('AWS execution identity required')
    result = run(client, bucket, event.get('request_id') or event.get('id') or execution_id, execution_id,
        recover_run=event.get('recover_run'), publish=publish_current)
    return {'statusCode': 200 if result['status'] == 'complete' else 202 if result['status'] == 'running' else 500,
        'body': json.dumps(result)}
