"""Public scenario availability; HTTP reads never trigger model publication."""
import json
import boto3
from scenario_publication import CURRENT, read, run

BUCKET = 'justhodl-dashboard-live'
s3 = boto3.client('s3', region_name='us-east-1')


def lambda_handler(event=None, context=None):
    event = event or {}
    if 'requestContext' in event or 'httpMethod' in event:
        raw, _ = read(s3, BUCKET, CURRENT)
        return {'statusCode': 200, 'headers': {'Content-Type': 'application/json', 'Cache-Control': 'no-store'}, 'body': raw.decode()}
    result = run(s3, BUCKET)
    return {'statusCode': 200, 'body': json.dumps(result)}
