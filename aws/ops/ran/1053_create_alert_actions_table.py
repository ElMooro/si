#!/usr/bin/env python3
"""ops 1053 — create DDB table justhodl-alert-actions for Behavior Mirror"""
import json, boto3, os, time
from datetime import datetime, timezone

ddb = boto3.client('dynamodb', region_name='us-east-1')

TABLE = 'justhodl-alert-actions'
report = {'started_at': datetime.now(timezone.utc).isoformat()}

try:
    ddb.describe_table(TableName=TABLE)
    report['table_status'] = 'ALREADY_EXISTS'
except ddb.exceptions.ResourceNotFoundException:
    ddb.create_table(
        TableName=TABLE,
        KeySchema=[
            {'AttributeName': 'alert_id', 'KeyType': 'HASH'},
            {'AttributeName': 'action_ts', 'KeyType': 'RANGE'},
        ],
        AttributeDefinitions=[
            {'AttributeName': 'alert_id', 'AttributeType': 'S'},
            {'AttributeName': 'action_ts', 'AttributeType': 'S'},
            {'AttributeName': 'engine', 'AttributeType': 'S'},
            {'AttributeName': 'logged_at', 'AttributeType': 'S'},
        ],
        GlobalSecondaryIndexes=[
            {
                'IndexName': 'engine-time-idx',
                'KeySchema': [
                    {'AttributeName': 'engine', 'KeyType': 'HASH'},
                    {'AttributeName': 'logged_at', 'KeyType': 'RANGE'},
                ],
                'Projection': {'ProjectionType': 'ALL'},
            },
        ],
        BillingMode='PAY_PER_REQUEST',
        Tags=[
            {'Key': 'project', 'Value': 'justhodl-behavior-mirror'},
            {'Key': 'exponential_idea', 'Value': '4'},
        ],
    )
    # Wait for active
    waiter = ddb.get_waiter('table_exists')
    waiter.wait(TableName=TABLE, WaiterConfig={'Delay': 5, 'MaxAttempts': 30})
    report['table_status'] = 'CREATED'

# Verify final state
desc = ddb.describe_table(TableName=TABLE)['Table']
report['table'] = {
    'arn': desc['TableArn'],
    'status': desc['TableStatus'],
    'attrs': [a['AttributeName']+':'+a['AttributeType'] for a in desc['AttributeDefinitions']],
    'keys': [k['AttributeName']+':'+k['KeyType'] for k in desc['KeySchema']],
    'gsi': [g['IndexName'] for g in desc.get('GlobalSecondaryIndexes',[])],
    'billing': desc.get('BillingModeSummary', {}).get('BillingMode'),
}

os.makedirs('aws/ops/reports', exist_ok=True)
with open('aws/ops/reports/1053.json', 'w') as f:
    json.dump(report, f, indent=2, default=str)
print(json.dumps(report, indent=2, default=str))
