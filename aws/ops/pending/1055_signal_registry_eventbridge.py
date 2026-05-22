#!/usr/bin/env python3
"""
ops 1055 — Switch signal-registry to EventBridge fan-out

S3 has 8 existing prefix-specific notifications for openbb-websocket-broadcast
that conflict with our data/ wildcard. Fix: enable EventBridge mode on the
bucket (which sends ALL events as EventBridge events) + create an EB rule
that matches data/*.json PutObject and targets signal-registry-ingest.

This is the AWS-recommended pattern for multi-consumer S3 events.
The 8 existing prefix notifications continue to work in parallel.
"""
import json, boto3, os, time
from datetime import datetime, timezone

REGION = 'us-east-1'
ACCOUNT = '857687956942'
BUCKET = 'justhodl-dashboard-live'
INGEST_LAMBDA = 'justhodl-signal-registry-ingest'
EB_RULE = 'justhodl-signal-registry-s3-events'

s3 = boto3.client('s3', region_name=REGION)
events = boto3.client('events', region_name=REGION)
lam = boto3.client('lambda', region_name=REGION)

report = {'started_at': datetime.now(timezone.utc).isoformat(), 'steps': []}

def step(name, fn):
    try:
        r = fn()
        report['steps'].append({'step': name, 'result': 'OK', 'detail': str(r)[:300]})
        print(f"  ✅ {name}")
        return r
    except Exception as e:
        report['steps'].append({'step': name, 'result': 'ERROR', 'error': str(e)[:300]})
        print(f"  ❌ {name}: {str(e)[:200]}")
        return None

# 1. Remove our half-installed S3 notification (the one from ops 1052)
print("[1] Cleaning up partial S3 notification config...")
def cleanup():
    existing = s3.get_bucket_notification_configuration(Bucket=BUCKET)
    lambda_configs = existing.get('LambdaFunctionConfigurations', [])
    cleaned = [c for c in lambda_configs if c.get('Id') != 'signal-registry-ingest']
    if len(cleaned) != len(lambda_configs):
        new_config = {'LambdaFunctionConfigurations': cleaned}
        for k in ('TopicConfigurations', 'QueueConfigurations', 'EventBridgeConfiguration'):
            if k in existing: new_config[k] = existing[k]
        s3.put_bucket_notification_configuration(Bucket=BUCKET, NotificationConfiguration=new_config)
        return f'removed {len(lambda_configs)-len(cleaned)} stale config(s)'
    return 'nothing to clean'
step('cleanup_stale_notif', cleanup)

# 2. Enable EventBridge events on the bucket (alongside existing notifications)
print("[2] Enable EventBridge mode on bucket...")
def enable_eb():
    existing = s3.get_bucket_notification_configuration(Bucket=BUCKET)
    # Preserve existing Lambda configs, add EventBridge enable
    new_config = {
        'LambdaFunctionConfigurations': existing.get('LambdaFunctionConfigurations', []),
        'EventBridgeConfiguration': {},  # empty dict = enable
    }
    for k in ('TopicConfigurations', 'QueueConfigurations'):
        if k in existing: new_config[k] = existing[k]
    s3.put_bucket_notification_configuration(Bucket=BUCKET, NotificationConfiguration=new_config)
    return 'EventBridge mode enabled'
step('enable_eventbridge', enable_eb)

# 3. Create EventBridge rule matching data/*.json PutObject
print("[3] Create EventBridge rule...")
def create_rule():
    pattern = {
        "source": ["aws.s3"],
        "detail-type": ["Object Created"],
        "detail": {
            "bucket": {"name": [BUCKET]},
            "object": {"key": [{"wildcard": "data/*.json"}]},
        },
    }
    resp = events.put_rule(
        Name=EB_RULE,
        EventPattern=json.dumps(pattern),
        State='ENABLED',
        Description='Route S3 data/*.json PutObject events to signal-registry-ingest',
    )
    return resp.get('RuleArn')
rule_arn = step('create_rule', create_rule)

# 4. Add Lambda as target
print("[4] Add Lambda as rule target...")
def add_target():
    events.put_targets(
        Rule=EB_RULE,
        Targets=[{
            'Id': 'signal-registry-ingest',
            'Arn': f'arn:aws:lambda:{REGION}:{ACCOUNT}:function:{INGEST_LAMBDA}',
        }],
    )
    return 'target added'
step('add_target', add_target)

# 5. Grant EventBridge permission to invoke Lambda
print("[5] Lambda invoke permission for EventBridge...")
def add_invoke():
    try:
        lam.add_permission(
            FunctionName=INGEST_LAMBDA,
            StatementId='AllowEventBridgeInvoke',
            Action='lambda:InvokeFunction',
            Principal='events.amazonaws.com',
            SourceArn=f'arn:aws:events:{REGION}:{ACCOUNT}:rule/{EB_RULE}',
        )
        return 'permission added'
    except lam.exceptions.ResourceConflictException:
        return 'already exists'
step('lambda_eb_perm', add_invoke)

# Also remove the now-unused S3 invoke permission (we're not using direct S3→Lambda anymore)
print("[6] Remove stale S3 invoke permission...")
def remove_s3():
    try:
        lam.remove_permission(FunctionName=INGEST_LAMBDA, StatementId='AllowS3Invoke')
        return 'removed'
    except lam.exceptions.ResourceNotFoundException:
        return 'not present'
step('remove_s3_perm', remove_s3)

# 7. Lambda needs to handle BOTH S3 direct event AND EventBridge S3 event payload formats.
# EventBridge wraps S3 events differently — the payload has top-level `source`, `detail-type`,
# and the S3 object info is at `detail.object` not `Records[].s3.object`.
# Update the Lambda code to handle EventBridge format.
print("[7] Update Lambda code to handle EventBridge event shape...")
import zipfile, io

NEW_CODE = r'''"""
justhodl-signal-registry-ingest

Handles both:
  - Direct S3 event format: {Records: [{s3: {bucket, object: {key, size, eTag}}}]}
  - EventBridge S3 event format: {source: "aws.s3", detail-type: "Object Created",
    detail: {bucket: {name}, object: {key, size, etag, sequencer}}}
"""
import json, os, boto3
from datetime import datetime, timezone

ddb = boto3.client('dynamodb')
s3  = boto3.client('s3')
TABLE = os.environ.get('REGISTRY_TABLE', 'justhodl-signal-registry')
SKIP_PREFIXES = ('data/_archive/', 'data/archive/', 'data/_registry/',
                 'data/snapshots/', 'data/_pit/', 'data/_registry_test')

def extract_signal_id(key):
    rest = key[len('data/'):] if key.startswith('data/') else key
    if '.' in rest:
        rest = rest.rsplit('.', 1)[0]
    return rest

def write_record(bucket, key, size, etag, ts, sequencer, source, event_name):
    if any(key.startswith(p) for p in SKIP_PREFIXES):
        return 'skipped'
    if not key.startswith('data/'):
        return 'skipped'
    
    signal_id = extract_signal_id(key)
    
    # Schema hint
    schema_hint = None
    try:
        if size < 10_000_000:
            obj = s3.get_object(Bucket=bucket, Key=key, Range='bytes=0-8191')
            text = obj['Body'].read().decode('utf-8', errors='replace')
            if text.lstrip().startswith('{'):
                # Naive top-level keys extraction without full parse
                import re
                top_keys = re.findall(r'^\s*"([^"]+)"\s*:', text, re.M)
                if top_keys:
                    schema_hint = sorted(set(top_keys))[:30]
            elif text.lstrip().startswith('['):
                schema_hint = 'list'
    except Exception:
        pass
    
    item = {
        'signal_id': {'S': signal_id},
        'ts':        {'S': ts},
        'output_key': {'S': key},
        'output_size': {'N': str(size)},
        'output_etag': {'S': etag.strip('"') if etag else ''},
        'sequencer': {'S': sequencer or ''},
        'event_source': {'S': source},
        'event_name': {'S': event_name},
        'recorded_at': {'S': datetime.now(timezone.utc).isoformat()},
        'engine_sha': {'S': 'unknown'},
    }
    if schema_hint:
        item['schema_hint'] = {'S': json.dumps(schema_hint)[:1024]}
    
    ddb.put_item(TableName=TABLE, Item=item)
    return 'ingested'

def lambda_handler(event, context):
    ingested = 0; skipped = 0; errors = 0
    
    # Detect format
    if event.get('source') == 'aws.s3' and event.get('detail-type') == 'Object Created':
        # EventBridge format
        try:
            d = event['detail']
            bucket = d['bucket']['name']
            obj = d['object']
            res = write_record(
                bucket=bucket, key=obj['key'], size=int(obj.get('size', 0)),
                etag=obj.get('etag', ''), ts=event['time'],
                sequencer=obj.get('sequencer'), source='eventbridge',
                event_name=event.get('detail-type', 'unknown'),
            )
            if res == 'ingested': ingested += 1
            else: skipped += 1
        except Exception as e:
            errors += 1
            print(f"[registry] EB error: {e}")
    
    elif 'Records' in event:
        # Direct S3 event format (legacy / unused after this ship)
        for r in event['Records']:
            try:
                s3evt = r.get('s3', {})
                bucket = s3evt.get('bucket', {}).get('name')
                obj = s3evt.get('object', {})
                key = obj.get('key')
                if not key:
                    skipped += 1; continue
                res = write_record(
                    bucket=bucket, key=key, size=int(obj.get('size', 0)),
                    etag=obj.get('eTag', ''), ts=r.get('eventTime') or datetime.now(timezone.utc).isoformat(),
                    sequencer=obj.get('sequencer'), source='s3-direct',
                    event_name=r.get('eventName', 'unknown'),
                )
                if res == 'ingested': ingested += 1
                else: skipped += 1
            except Exception as e:
                errors += 1
                print(f"[registry] S3 direct error: {e}")
    else:
        print(f"[registry] unknown event shape: {list(event.keys())[:5]}")
    
    print(f"[registry] ingested={ingested} skipped={skipped} errors={errors}")
    return {'statusCode': 200, 'ingested': ingested, 'skipped': skipped, 'errors': errors}
'''

def update_code():
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, 'w', zipfile.ZIP_DEFLATED) as z:
        z.writestr('lambda_function.py', NEW_CODE)
    lam.update_function_code(FunctionName=INGEST_LAMBDA, ZipFile=buf.getvalue())
    return 'code updated'
step('update_lambda_code', update_code)
time.sleep(8)  # wait for update propagation

# 8. Live verify
print("[8] Live verify via EventBridge path...")
def verify():
    test_key = 'data/_registry_eb_test.json'
    test_payload = {'test': 'eventbridge-path', 'ts': datetime.now(timezone.utc).isoformat()}
    s3.put_object(Bucket=BUCKET, Key=test_key,
                  Body=json.dumps(test_payload).encode(), ContentType='application/json')
    time.sleep(15)  # EB has more latency than direct S3
    
    ddb = boto3.client('dynamodb', region_name=REGION)
    # Note: signal_id = "_registry_eb_test" (since extract strips .json)
    resp = ddb.query(
        TableName='justhodl-signal-registry',
        KeyConditionExpression='signal_id = :s',
        ExpressionAttributeValues={':s': {'S': '_registry_eb_test'}},
        Limit=5, ScanIndexForward=False,
    )
    items = resp.get('Items', [])
    try:
        s3.delete_object(Bucket=BUCKET, Key=test_key)
    except: pass
    return f'{len(items)} record(s) found, latest_etag={items[0].get("output_etag",{}).get("S","?") if items else "n/a"}'
step('live_verify_eb', verify)

# Summary
report['completed_at'] = datetime.now(timezone.utc).isoformat()
report['summary'] = {
    'all_ok': all(s.get('result') == 'OK' for s in report['steps']),
    'n_steps': len(report['steps']),
    'n_ok': sum(1 for s in report['steps'] if s.get('result') == 'OK'),
}

os.makedirs('aws/ops/reports', exist_ok=True)
with open('aws/ops/reports/1055.json', 'w') as f:
    json.dump(report, f, indent=2, default=str)
print(f"\n=== SUMMARY ===")
print(json.dumps(report['summary'], indent=2))
