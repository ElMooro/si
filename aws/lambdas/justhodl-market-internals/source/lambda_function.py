"""Public native breadth producer; legacy rolling-cache scoring is audit-only."""
import json
import boto3
from botocore.config import Config
from massive import get_massive_key
from breadth_research_store import run, now
from breadth_research_model import sessions, CONTRACT


def lambda_handler(event=None, context=None):
    # No event can enable legacy scoring, private account reads or notifications.
    event = event if isinstance(event, dict) else {}
    if event.get('validate_only') is True:
        return {'statusCode': 200, 'body': json.dumps({'validation_only': True, 'contract': CONTRACT,
            'reviewed_calendar_latest_session': sessions(now())[-1], 'published': False})}
    execution_id = getattr(context, 'aws_request_id', None)
    if not execution_id: raise ValueError('AWS execution identity required')
    request_id = event.get('request_id', execution_id)
    remaining = context.get_remaining_time_in_millis()/1000
    result = run(boto3.client('s3', region_name='us-east-1', config=Config(connect_timeout=5, read_timeout=30,
        retries={'max_attempts': 2}, max_pool_connections=8, tcp_keepalive=True)),
        'justhodl-dashboard-live', request_id, execution_id, get_massive_key, remaining)
    return {'statusCode': 200 if result['status'] == 'complete' else 202 if result['status'] == 'running' else 500,
        'body': json.dumps(result)}
