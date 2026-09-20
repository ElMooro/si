"""Public original-source Crisis Plumbing; legacy scoring is audit-only."""
import json
import boto3
from plumbing_research_store import run


def lambda_handler(event=None,context=None):
    # Event flags cannot enable legacy forecasts, private contexts or notifications.
    result=run(boto3.client('s3',region_name='us-east-1'),'justhodl-dashboard-live',
               validation_only=isinstance(event,dict) and event.get('validate_only') is True)
    return {'statusCode':200 if result.get('published') or result.get('validation_only') else 409,
            'body':json.dumps(result)}
