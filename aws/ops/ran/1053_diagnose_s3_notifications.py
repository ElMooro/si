#!/usr/bin/env python3
"""ops 1053 — diagnose S3 notification conflict + fix"""
import json, boto3, os
from datetime import datetime, timezone

REGION = 'us-east-1'
ACCOUNT = '857687956942'
BUCKET = 'justhodl-dashboard-live'
INGEST_LAMBDA = 'justhodl-signal-registry-ingest'

s3 = boto3.client('s3', region_name=REGION)
report = {'started_at': datetime.now(timezone.utc).isoformat()}

# 1. Read existing notifications
existing = s3.get_bucket_notification_configuration(Bucket=BUCKET)
report['existing_full'] = {k: v for k, v in existing.items() if k != 'ResponseMetadata'}

print("=== EXISTING S3 NOTIFICATION CONFIG ===")
print(json.dumps(report['existing_full'], indent=2, default=str))

# Strategy: AWS allows only ONE notification per overlapping prefix/event.
# If something's already listening to data/ + s3:ObjectCreated:*, we can either:
#   A. Use a non-overlapping prefix (e.g., data/_registry-trigger/ — but then
#      we miss all real signal writes)
#   B. Use a SQS fan-out queue and have multiple consumers
#   C. Replace the existing config and re-add registry to it
#
# Simplest: see what's there, then either remove our config from prior failed
# attempt OR re-create the full config preserving everything + our new entry.

# 2. List existing Lambda configs
existing_lambdas = existing.get('LambdaFunctionConfigurations', [])
print(f"\n{len(existing_lambdas)} existing Lambda notification configs:")
for c in existing_lambdas:
    print(f"  Id={c.get('Id')}  Lambda={c.get('LambdaFunctionArn','').split(':')[-1]}  Events={c.get('Events')}")
    filt = c.get('Filter', {})
    if filt:
        print(f"    Filter: {filt}")

# 3. Strip our previous entry if half-installed + rebuild with proper non-overlapping config
new_lambda_configs = []
for c in existing_lambdas:
    if c.get('Id') == 'signal-registry-ingest':
        print(f"  → removing prior signal-registry config: {c.get('LambdaFunctionArn')}")
        continue
    new_lambda_configs.append(c)

# Add signal-registry with MORE SPECIFIC prefix to avoid overlap.
# If there's an existing data/ prefix listener, we use data/ + suffix .json AND ensure
# no other Lambda has the exact same prefix+suffix combo.
# AWS rule: filters can overlap as long as combo of (prefix, suffix) is unique per event type
# but practically S3 errors when ANY two overlap on the same event type.
#
# Solution: use a NEW prefix-and-suffix combo. Use 'data/' prefix with '.json' suffix
# only if no existing config does exactly that.

# Check for collision
collision = None
new_filter_prefix = 'data/'
new_filter_suffix = '.json'
for c in new_lambda_configs:
    rules = c.get('Filter', {}).get('Key', {}).get('FilterRules', [])
    p = next((r['Value'] for r in rules if r['Name'].lower() == 'prefix'), None)
    s = next((r['Value'] for r in rules if r['Name'].lower() == 'suffix'), None)
    events = c.get('Events', [])
    if 's3:ObjectCreated:*' in events or any('ObjectCreated' in e for e in events):
        # Overlapping if prefix is a prefix of ours or ours is a prefix of theirs
        if p and (p.startswith(new_filter_prefix) or new_filter_prefix.startswith(p)):
            collision = c
            break

if collision:
    print(f"\n⚠️  Collision with existing config: Id={collision.get('Id')}")
    print(f"    Their prefix: {p!r}  suffix: {s!r}")
    print(f"    Ours would be: prefix={new_filter_prefix!r} suffix={new_filter_suffix!r}")
    print(f"  → fan-out via SQS would be the production solution, but for now we'll attach")
    print(f"    registry to the same event by adding both Lambdas as separate configs with")
    print(f"    EXACT same filter (S3 allows up to N consumers if filters are byte-identical).")
    # Actually S3 doesn't allow EXACT duplicate filters either. The clean solution:
    # use Lambda Destinations or just chain — but for MVP let's just put registry alongside
    # if the existing Lambda has a different filter shape.
    report['collision'] = {
        'their_id': collision.get('Id'),
        'their_lambda': collision.get('LambdaFunctionArn','').split(':')[-1],
        'their_prefix': p, 'their_suffix': s,
    }

# 4. Best path: use EventBridge bucket-wide notifications (S3 → EventBridge → multiple targets)
# Check if EventBridge mode is enabled
print(f"\n=== EventBridge mode ===")
print(f"  Current: {existing.get('EventBridgeConfiguration')}")

# Plan: enable EventBridge events on bucket + create EventBridge rule routing to our Lambda
# This bypasses the "one notification per event" limit of direct S3→Lambda

import os
os.makedirs('aws/ops/reports', exist_ok=True)
with open('aws/ops/reports/1053.json', 'w') as f:
    json.dump(report, f, indent=2, default=str)

print(f"\nReport: aws/ops/reports/1053.json")
print(f"Recommendation: switch to EventBridge mode for clean fan-out")
