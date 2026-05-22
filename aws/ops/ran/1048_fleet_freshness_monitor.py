#!/usr/bin/env python3
"""
ops 1048 — Deploy fleet-freshness-monitor

Complements fleet-error-monitor. Catches the OTHER class of silent
failure: Lambdas that invoke successfully (no error in CW metrics)
but fail to write their output to S3.

Strategy:
  - For each data/* key referenced by an HTML page, check S3 LastModified
  - If older than the threshold (default 26h for daily, 2h for hourly),
    alert via SNS + Telegram
  - Excludes archive/snapshot paths
  - Excludes admin-config files (intentionally infrequent)

Threshold inference:
  - Default 26h (one missed daily run)
  - Custom thresholds via a manifest at data/_freshness-manifest.json
    (Lambda creates a default if missing)
"""
import json, boto3, os, time, zipfile, io
from datetime import datetime, timezone

REGION = 'us-east-1'
ACCOUNT = '857687956942'
DLQ_ARN = f'arn:aws:sqs:{REGION}:{ACCOUNT}:justhodl-dlq-default'
SNS_ARN = f'arn:aws:sns:{REGION}:{ACCOUNT}:justhodl-fleet-alerts'
LAMBDA_ROLE = f'arn:aws:iam::{ACCOUNT}:role/lambda-execution-role'
BUCKET = 'justhodl-dashboard-live'

lam = boto3.client('lambda', region_name=REGION)
events = boto3.client('events', region_name=REGION)
s3 = boto3.client('s3', region_name=REGION)

report = {'started_at': datetime.now(timezone.utc).isoformat(), 'steps': []}


def step(name, fn):
    try:
        r = fn()
        report['steps'].append({'step': name, 'result': 'OK'})
        print(f"  ✅ {name}")
        return r
    except Exception as e:
        report['steps'].append({'step': name, 'result': 'ERROR', 'error': str(e)[:300]})
        print(f"  ❌ {name}: {str(e)[:180]}")
        return None


# Lambda code (inline)
LAMBDA_CODE = '''
import json, os, boto3, urllib.request, re
from datetime import datetime, timezone

REGION = "us-east-1"
BUCKET = os.environ.get("BUCKET", "justhodl-dashboard-live")
SNS_ARN = os.environ["SNS_ARN"]
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")
DEFAULT_MAX_AGE_H = float(os.environ.get("DEFAULT_MAX_AGE_H", "26"))
MANIFEST_KEY = "data/_freshness-manifest.json"

s3 = boto3.client("s3", region_name=REGION)
sns = boto3.client("sns", region_name=REGION)

# Default manifest with known schedule cadences for critical feeds.
# Override these by uploading data/_freshness-manifest.json.
DEFAULT_MANIFEST = {
    "rules": [
        {"prefix": "data/", "default_max_age_h": 26.0},
    ],
    "exclude_prefixes": [
        "data/archive/", "data/_archive/", "data/snapshots/",
        "data/secretary-history/", "data/calibration-history/",
    ],
    "admin_only_keys": [
        "data/khalid-config.json", "data/ka-config.json",
    ],
    "key_overrides": {
        # Hourly
        "data/report.json": 2.0,
        # Every 5 min
        "data/options-flow.json": 0.2,
        # Weekly (Sunday)
        "data/factor-decomposition.json": 192.0,
        "data/cftc-deep-view.json": 192.0,
    },
}


def load_manifest():
    try:
        obj = s3.get_object(Bucket=BUCKET, Key=MANIFEST_KEY)
        return json.loads(obj["Body"].read())
    except s3.exceptions.NoSuchKey:
        # Create default
        s3.put_object(
            Bucket=BUCKET, Key=MANIFEST_KEY,
            Body=json.dumps(DEFAULT_MANIFEST, indent=2).encode(),
            ContentType="application/json",
        )
        return DEFAULT_MANIFEST
    except Exception as e:
        print(f"manifest load failed: {e}")
        return DEFAULT_MANIFEST


def lambda_handler(event, context):
    now = datetime.now(timezone.utc)
    manifest = load_manifest()
    
    exclude = manifest.get("exclude_prefixes", [])
    admin_only = set(manifest.get("admin_only_keys", []))
    overrides = manifest.get("key_overrides", {})
    default_max = float(manifest.get("rules", [{}])[0].get("default_max_age_h", DEFAULT_MAX_AGE_H))
    
    stale = []
    scanned = 0
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=BUCKET, Prefix="data/"):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            scanned += 1
            if any(key.startswith(p) for p in exclude):
                continue
            if key in admin_only:
                continue
            age_h = (now - obj["LastModified"]).total_seconds() / 3600
            threshold = overrides.get(key, default_max)
            if age_h > threshold:
                stale.append({
                    "key": key, "age_h": round(age_h, 1),
                    "threshold_h": threshold,
                    "ratio": round(age_h / threshold, 2),
                    "size": obj["Size"],
                })
    
    stale.sort(key=lambda x: -x["ratio"])
    
    # Critical = >3x threshold
    critical = [s for s in stale if s["ratio"] >= 3.0]
    
    summary = {
        "checked_at": now.isoformat(),
        "n_keys_scanned": scanned,
        "n_stale_total": len(stale),
        "n_critical": len(critical),
        "stale_top_20": stale[:20],
        "thresholds_used": {"default_h": default_max, "n_overrides": len(overrides)},
    }
    
    # Alert if 5+ critical
    if len(critical) >= 5:
        lines = [f"\\u26a0\\ufe0f *JustHodl Freshness Alert*",
                 f"_{len(critical)} keys staler than 3x expected_", ""]
        for s in critical[:10]:
            lines.append(f"\\u2022 `{s['key']}`: {s['age_h']}h (threshold {s['threshold_h']}h, {s['ratio']}x)")
        if len(critical) > 10:
            lines.append(f"...and {len(critical)-10} more")
        msg = "\\n".join(lines)
        
        try:
            sns.publish(TopicArn=SNS_ARN, Subject=f"Freshness alert: {len(critical)} stale keys",
                        Message=msg)
        except Exception as e:
            summary["sns_error"] = str(e)[:200]
        
        if TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID:
            try:
                url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
                data = json.dumps({"chat_id": TELEGRAM_CHAT_ID, "text": msg, "parse_mode": "Markdown"}).encode()
                req = urllib.request.Request(url, data=data, headers={"Content-Type": "application/json"})
                urllib.request.urlopen(req, timeout=10)
            except Exception as e:
                summary["telegram_error"] = str(e)[:200]
    
    # Save summary to S3 for the alarms.html or fleet-status page
    s3.put_object(
        Bucket=BUCKET,
        Key="data/_freshness-status.json",
        Body=json.dumps(summary, default=str, indent=2).encode(),
        ContentType="application/json",
        CacheControl="max-age=60",
    )
    return summary
'''

# Build zip
zbuf = io.BytesIO()
with zipfile.ZipFile(zbuf, 'w', zipfile.ZIP_DEFLATED) as z:
    info = zipfile.ZipInfo('lambda_function.py')
    info.external_attr = 0o644 << 16
    z.writestr(info, LAMBDA_CODE)
zip_bytes = zbuf.getvalue()
print(f"Zip size: {len(zip_bytes)} bytes")

FN_NAME = 'justhodl-fleet-freshness-monitor'
create_kwargs = {
    'FunctionName': FN_NAME,
    'Runtime': 'python3.12',
    'Role': LAMBDA_ROLE,
    'Handler': 'lambda_function.lambda_handler',
    'Code': {'ZipFile': zip_bytes},
    'Timeout': 300,
    'MemorySize': 512,
    'Environment': {'Variables': {
        'BUCKET': BUCKET,
        'SNS_ARN': SNS_ARN,
        'TELEGRAM_BOT_TOKEN': os.environ.get('TELEGRAM_BOT_TOKEN', ''),
        'TELEGRAM_CHAT_ID': '241451060',
        'DEFAULT_MAX_AGE_H': '26',
    }},
    'TracingConfig': {'Mode': 'Active'},
    'DeadLetterConfig': {'TargetArn': DLQ_ARN},
}

print("[1] Deploy fleet-freshness-monitor...")
try:
    lam.get_function(FunctionName=FN_NAME)
    step('update_code', lambda: lam.update_function_code(FunctionName=FN_NAME, ZipFile=zip_bytes))
    time.sleep(3)
    step('update_config', lambda: lam.update_function_configuration(
        FunctionName=FN_NAME,
        Timeout=create_kwargs['Timeout'],
        MemorySize=create_kwargs['MemorySize'],
        Environment=create_kwargs['Environment'],
        TracingConfig=create_kwargs['TracingConfig'],
        DeadLetterConfig=create_kwargs['DeadLetterConfig'],
    ))
except lam.exceptions.ResourceNotFoundException:
    step('create_fn', lambda: lam.create_function(**create_kwargs))

# EB rule (every 30 min)
print("[2] Schedule (every 30 min)...")
RULE = 'justhodl-fleet-freshness-monitor-30min'
step('put_rule', lambda: events.put_rule(
    Name=RULE,
    ScheduleExpression='rate(30 minutes)',
    State='ENABLED',
    Description='Scan all data/ S3 keys for staleness vs expected schedule',
))
step('add_perm', lambda: lam.add_permission(
    FunctionName=FN_NAME, StatementId='allow-eb-freshness',
    Action='lambda:InvokeFunction', Principal='events.amazonaws.com',
    SourceArn=f'arn:aws:events:{REGION}:{ACCOUNT}:rule/{RULE}',
))
step('put_target', lambda: events.put_targets(
    Rule=RULE,
    Targets=[{'Id': '1', 'Arn': f'arn:aws:lambda:{REGION}:{ACCOUNT}:function:{FN_NAME}'}],
))

# Test invoke
print("[3] Test invoke...")
try:
    inv = lam.invoke(FunctionName=FN_NAME, InvocationType='RequestResponse', Payload=b'{}')
    payload = inv['Payload'].read().decode()
    parsed = json.loads(payload) if payload.startswith('{') else payload
    report['test_invoke'] = {
        'status': inv['StatusCode'],
        'function_error': inv.get('FunctionError', 'none'),
        'response_summary': {
            'n_keys_scanned': parsed.get('n_keys_scanned') if isinstance(parsed, dict) else None,
            'n_stale_total': parsed.get('n_stale_total') if isinstance(parsed, dict) else None,
            'n_critical': parsed.get('n_critical') if isinstance(parsed, dict) else None,
            'top_5_stale': parsed.get('stale_top_20', [])[:5] if isinstance(parsed, dict) else None,
        },
    }
except Exception as e:
    report['test_invoke'] = {'error': str(e)[:300]}

report['completed_at'] = datetime.now(timezone.utc).isoformat()
os.makedirs('aws/ops/reports', exist_ok=True)
with open('aws/ops/reports/1048.json', 'w') as f:
    json.dump(report, f, indent=2, default=str)

print(f"\n=== FRESHNESS MONITOR DEPLOYED ===")
print(json.dumps(report.get('test_invoke', {}).get('response_summary', {}), indent=2, default=str))
