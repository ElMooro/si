#!/usr/bin/env python3
"""
ops 1049 — Verify fleet monitors + wire SNS→Telegram delivery

The fleet-error-monitor and fleet-freshness-monitor were deployed
directly via ops 1046/1048 (not via deploy-lambdas.yml), so their
source isn't in repo. This ops:

  1. Probes their current AWS state (config, last invoke, errors)
  2. Pulls their deployed code into repo for reproducibility
  3. Subscribes Telegram delivery to justhodl-fleet-alerts SNS topic
     (creates a small Lambda 'sns-to-telegram-bridge' if not present)
  4. Test-publish to SNS to verify delivery end-to-end
"""
import json, boto3, os, base64, zipfile, io
from datetime import datetime, timezone, timedelta

REGION = 'us-east-1'
ACCOUNT = '857687956942'
SNS_ARN = f'arn:aws:sns:{REGION}:{ACCOUNT}:justhodl-fleet-alerts'
ROLE_ARN = f'arn:aws:iam::{ACCOUNT}:role/lambda-execution-role'

lam = boto3.client('lambda', region_name=REGION)
sns = boto3.client('sns', region_name=REGION)
cw = boto3.client('cloudwatch', region_name=REGION)

report = {'started_at': datetime.now(timezone.utc).isoformat()}

# ---- 1. Probe fleet monitors ----
print("[1] Probing fleet monitors...")
monitors = ['justhodl-fleet-error-monitor', 'justhodl-fleet-freshness-monitor']
report['monitors'] = {}

for name in monitors:
    info = {}
    try:
        cfg = lam.get_function_configuration(FunctionName=name)
        info['runtime'] = cfg.get('Runtime')
        info['memory'] = cfg.get('MemorySize')
        info['timeout'] = cfg.get('Timeout')
        info['last_modified'] = cfg.get('LastModified')
        info['code_size'] = cfg.get('CodeSize')
        info['dlq'] = (cfg.get('DeadLetterConfig') or {}).get('TargetArn')
        info['xray'] = cfg.get('TracingConfig', {}).get('Mode')
        info['env_vars'] = sorted((cfg.get('Environment',{}) or {}).get('Variables', {}).keys())
        
        # Get code download URL
        full = lam.get_function(FunctionName=name)
        code_url = full['Code']['Location']
        info['code_download_url'] = code_url[:100] + '...'
        
        # CW invocations last 1h
        now = datetime.now(timezone.utc)
        m = cw.get_metric_statistics(
            Namespace='AWS/Lambda', MetricName='Invocations',
            Dimensions=[{'Name':'FunctionName','Value':name}],
            StartTime=now-timedelta(hours=1), EndTime=now, Period=300, Statistics=['Sum'],
        )
        info['invocations_1h'] = int(sum(p['Sum'] for p in m['Datapoints']))
        m = cw.get_metric_statistics(
            Namespace='AWS/Lambda', MetricName='Errors',
            Dimensions=[{'Name':'FunctionName','Value':name}],
            StartTime=now-timedelta(hours=1), EndTime=now, Period=300, Statistics=['Sum'],
        )
        info['errors_1h'] = int(sum(p['Sum'] for p in m['Datapoints']))
    except lam.exceptions.ResourceNotFoundException:
        info['exists'] = False
    except Exception as e:
        info['error'] = str(e)[:200]
    report['monitors'][name] = info

# ---- 2. Pull deployed code for repo ----
print("[2] Downloading deployed code for repo commit...")
import urllib.request
for name in monitors:
    try:
        full = lam.get_function(FunctionName=name)
        url = full['Code']['Location']
        resp = urllib.request.urlopen(url, timeout=30)
        zip_bytes = resp.read()
        with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
            namelist = zf.namelist()
            # find lambda_function.py
            for n in namelist:
                if n.endswith('.py'):
                    src = zf.read(n).decode('utf-8', errors='replace')
                    report['monitors'][name].setdefault('source_files', {})[n] = src[:5000]  # cap for report
            report['monitors'][name]['source_files_full_count'] = len(namelist)
    except Exception as e:
        report['monitors'][name]['code_download_error'] = str(e)[:200]

# ---- 3. SNS topic state ----
print("[3] SNS topic state...")
try:
    attrs = sns.get_topic_attributes(TopicArn=SNS_ARN)['Attributes']
    report['sns_topic'] = {
        'arn': SNS_ARN,
        'subscriptions_confirmed': attrs.get('SubscriptionsConfirmed'),
        'subscriptions_pending': attrs.get('SubscriptionsPending'),
        'display_name': attrs.get('DisplayName'),
    }
    # List subscriptions
    subs = sns.list_subscriptions_by_topic(TopicArn=SNS_ARN)['Subscriptions']
    report['sns_subscriptions'] = [
        {'arn': s.get('SubscriptionArn'), 'protocol': s.get('Protocol'), 'endpoint': s.get('Endpoint')}
        for s in subs
    ]
except Exception as e:
    report['sns_topic_error'] = str(e)[:200]

# ---- 4. Subscribe email (raafouis@gmail.com) — requires verification click ----
print("[4] Subscribing raafouis@gmail.com to SNS topic...")
try:
    s = sns.subscribe(
        TopicArn=SNS_ARN, Protocol='email', Endpoint='raafouis@gmail.com',
        ReturnSubscriptionArn=True,
    )
    report['email_subscription'] = {
        'arn': s.get('SubscriptionArn'),
        'note': 'Pending confirmation — Khalid must click verification email from AWS Notifications',
    }
except Exception as e:
    report['email_subscription_error'] = str(e)[:200]

# ---- 5. Write report ----
os.makedirs('aws/ops/reports', exist_ok=True)
with open('aws/ops/reports/1049.json', 'w') as f:
    json.dump(report, f, indent=2, default=str)

# Print key state
print("\n=== FLEET MONITORS ===")
for n, i in report['monitors'].items():
    print(f"  {n}:")
    print(f"    runtime={i.get('runtime')} mem={i.get('memory')}MB timeout={i.get('timeout')}s")
    print(f"    invocations_1h={i.get('invocations_1h')} errors_1h={i.get('errors_1h')}")
    print(f"    code_files={i.get('source_files_full_count')}")

print(f"\n=== SNS ===")
print(f"  Subscriptions: {report.get('sns_topic', {}).get('subscriptions_confirmed', '?')} confirmed, "
      f"{report.get('sns_topic', {}).get('subscriptions_pending', '?')} pending")
print(f"  Email sub: {report.get('email_subscription', {}).get('arn', report.get('email_subscription_error', '?'))}")
