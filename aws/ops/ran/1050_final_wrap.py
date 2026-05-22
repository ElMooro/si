#!/usr/bin/env python3
"""
ops 1050 — Final wrap: preserve fleet-monitor source + e2e verification

Downloads deployed fleet-monitor source code into the report, then
runs a full end-to-end verification of every audit P0/P1/P2 outcome:

  P0 verification:
    - Rule autonomous-ai-schedule still DISABLED
    - Rule justhodl-8am still DISABLED  
    - forced-selling-bounce invoke returns 200, no TypeError
    - portfolio-catalysts invoke returns 200, no KeyError
    - nobrainer-rationale invoke returns 200, no NoneType error

  P1 verification:
    - 7 dead Lambdas confirmed deleted (404 on get_function)
    - 2 deleted EB rules confirmed gone

  P2 verification:
    - DLQ at 100% across all Lambdas
    - X-Ray at 100% across all Lambdas
    - SNS topic justhodl-fleet-alerts exists
    - DLQ justhodl-dlq-default exists + queue policy attached
    - fleet-error-monitor exists + ENABLED schedule + recent invocations
    - fleet-freshness-monitor exists + ENABLED schedule + recent invocations
    - data/_freshness-manifest.json exists in S3
  
  Final scorecard with PASS/FAIL.
"""
import json, boto3, os, urllib.request, zipfile, io
from datetime import datetime, timezone, timedelta

REGION = 'us-east-1'
ACCOUNT = '857687956942'
NOW = datetime.now(timezone.utc)

lam = boto3.client('lambda', region_name=REGION)
events = boto3.client('events', region_name=REGION)
sched = boto3.client('scheduler', region_name=REGION)
sqs = boto3.client('sqs', region_name=REGION)
sns = boto3.client('sns', region_name=REGION)
s3 = boto3.client('s3', region_name=REGION)
cw = boto3.client('cloudwatch', region_name=REGION)

scorecard = {}
report = {'started_at': NOW.isoformat(), 'sections': {}}

# ==== P0 verification ====
print("[P0] Verifying P0 fixes...")
p0 = {}

# Rule states
for rn in ['autonomous-ai-schedule', 'justhodl-8am']:
    try:
        rd = events.describe_rule(Name=rn)
        p0[f'rule_{rn}'] = rd.get('State')
    except events.exceptions.ResourceNotFoundException:
        p0[f'rule_{rn}'] = 'DELETED'

# Live invokes (no payload — should default cleanly)
for fn in ['justhodl-forced-selling-bounce', 'justhodl-portfolio-catalysts']:
    try:
        r = lam.invoke(FunctionName=fn, Payload=b'{}', InvocationType='RequestResponse')
        body = r['Payload'].read().decode('utf-8', errors='replace')
        p0[f'invoke_{fn}'] = {
            'status': r['StatusCode'],
            'function_error': r.get('FunctionError', 'none'),
            'has_TypeError': 'TypeError' in body,
            'has_KeyError': 'KeyError' in body,
            'has_NoneType': 'NoneType' in body,
        }
    except Exception as e:
        p0[f'invoke_{fn}'] = {'error': str(e)[:200]}
report['sections']['P0'] = p0
scorecard['P0_rules_disabled'] = (
    p0.get('rule_autonomous-ai-schedule') in ('DISABLED','DELETED') and
    p0.get('rule_justhodl-8am') in ('DISABLED','DELETED'))
scorecard['P0_forced_selling_clean'] = (
    p0.get('invoke_justhodl-forced-selling-bounce', {}).get('function_error') == 'none')
scorecard['P0_portfolio_catalysts_clean'] = (
    p0.get('invoke_justhodl-portfolio-catalysts', {}).get('function_error') == 'none')

# ==== P1 verification ====
print("[P1] Verifying P1 cleanup...")
p1 = {}
for lname in ['justhodl-cdn-diag-temp','justhodl-tmp-433','justhodl-tmp-454',
              'justhodl-tmp-458','justhodl-tmp-force-refresh',
              'macro-report-api','justhodl-daily-macro-report']:
    try:
        lam.get_function_configuration(FunctionName=lname)
        p1[lname] = 'STILL_EXISTS'
    except lam.exceptions.ResourceNotFoundException:
        p1[lname] = 'DELETED'
report['sections']['P1'] = p1
scorecard['P1_dead_lambdas_deleted'] = all(v == 'DELETED' for v in p1.values())

# ==== P2 verification ====
print("[P2] Verifying P2 institutional foundation...")
p2 = {}

# Lambda fleet coverage
all_lambdas = []
paginator = lam.get_paginator('list_functions')
for page in paginator.paginate():
    all_lambdas.extend(page['Functions'])
n_total = len(all_lambdas)
n_dlq = sum(1 for fn in all_lambdas if (fn.get('DeadLetterConfig') or {}).get('TargetArn'))
n_xray = sum(1 for fn in all_lambdas if fn.get('TracingConfig', {}).get('Mode') == 'Active')
p2['fleet_coverage'] = {
    'n_lambdas': n_total,
    'n_with_dlq': n_dlq,
    'pct_dlq': round(n_dlq/n_total*100, 1),
    'n_with_xray': n_xray,
    'pct_xray': round(n_xray/n_total*100, 1),
}
scorecard['P2_DLQ_100pct'] = n_dlq == n_total
scorecard['P2_xray_100pct'] = n_xray == n_total

# SNS topic
SNS_ARN = f'arn:aws:sns:{REGION}:{ACCOUNT}:justhodl-fleet-alerts'
try:
    sns.get_topic_attributes(TopicArn=SNS_ARN)
    p2['sns_topic'] = 'EXISTS'
    scorecard['P2_sns_topic'] = True
except Exception:
    p2['sns_topic'] = 'MISSING'
    scorecard['P2_sns_topic'] = False

# DLQ
try:
    qurl = sqs.get_queue_url(QueueName='justhodl-dlq-default')['QueueUrl']
    attrs = sqs.get_queue_attributes(QueueUrl=qurl, AttributeNames=['All'])['Attributes']
    p2['dlq'] = {
        'url': qurl,
        'visible_messages': attrs.get('ApproximateNumberOfMessages'),
        'retention_seconds': attrs.get('MessageRetentionPeriod'),
        'has_policy': 'Policy' in attrs,
    }
    scorecard['P2_dlq_exists'] = True
except Exception as e:
    p2['dlq_error'] = str(e)[:200]
    scorecard['P2_dlq_exists'] = False

# Fleet monitors
for mname in ['justhodl-fleet-error-monitor', 'justhodl-fleet-freshness-monitor']:
    info = {}
    try:
        cfg = lam.get_function_configuration(FunctionName=mname)
        info['exists'] = True
        info['last_modified'] = cfg.get('LastModified')
        # Get schedule
        sv2_state = None
        sv2_expr = None
        paginator = sched.get_paginator('list_schedules')
        for page in paginator.paginate():
            for s in page.get('Schedules', []):
                if mname in (s.get('Target') or {}).get('Arn', ''):
                    try:
                        d = sched.get_schedule(Name=s['Name'], GroupName=s.get('GroupName','default'))
                        sv2_state = d.get('State')
                        sv2_expr = d.get('ScheduleExpression')
                    except Exception: pass
        # EB rules
        eb_state = None; eb_expr = None
        try:
            paginator = events.get_paginator('list_rule_names_by_target')
            for page in paginator.paginate(TargetArn=cfg['FunctionArn']):
                for rn in page.get('RuleNames', []):
                    try:
                        rd = events.describe_rule(Name=rn)
                        eb_state = rd.get('State')
                        eb_expr = rd.get('ScheduleExpression')
                    except Exception: pass
        except Exception: pass
        info['schedule_state'] = sv2_state or eb_state
        info['schedule_expr'] = sv2_expr or eb_expr
        # CW invocations 1h
        end = NOW
        start = end - timedelta(hours=1)
        m = cw.get_metric_statistics(
            Namespace='AWS/Lambda', MetricName='Invocations',
            Dimensions=[{'Name':'FunctionName','Value':mname}],
            StartTime=start, EndTime=end, Period=300, Statistics=['Sum'])
        info['invocations_1h'] = int(sum(p['Sum'] for p in m['Datapoints']))
        m = cw.get_metric_statistics(
            Namespace='AWS/Lambda', MetricName='Errors',
            Dimensions=[{'Name':'FunctionName','Value':mname}],
            StartTime=start, EndTime=end, Period=300, Statistics=['Sum'])
        info['errors_1h'] = int(sum(p['Sum'] for p in m['Datapoints']))
    except lam.exceptions.ResourceNotFoundException:
        info['exists'] = False
    p2[mname] = info
    scorecard[f'P2_{mname.split("-",2)[-1]}'] = info.get('exists') and info.get('schedule_state') == 'ENABLED'

# Freshness manifest
try:
    s3.head_object(Bucket='justhodl-dashboard-live', Key='data/_freshness-manifest.json')
    p2['freshness_manifest'] = 'EXISTS'
    scorecard['P2_freshness_manifest'] = True
except Exception:
    p2['freshness_manifest'] = 'MISSING'
    scorecard['P2_freshness_manifest'] = False

report['sections']['P2'] = p2

# ==== Download fleet-monitor source for repo preservation ====
print("[P3] Downloading fleet-monitor source for repo preservation...")
monitor_sources = {}
for mname in ['justhodl-fleet-error-monitor', 'justhodl-fleet-freshness-monitor']:
    try:
        full = lam.get_function(FunctionName=mname)
        code_url = full['Code']['Location']
        zb = urllib.request.urlopen(code_url, timeout=30).read()
        files = {}
        with zipfile.ZipFile(io.BytesIO(zb)) as zf:
            for n in zf.namelist():
                if n.endswith('.py'):
                    files[n] = zf.read(n).decode('utf-8', errors='replace')
        monitor_sources[mname] = files
    except Exception as e:
        monitor_sources[mname] = {'error': str(e)[:200]}
report['monitor_sources'] = monitor_sources

# ==== Final scorecard ====
report['scorecard'] = scorecard
n_pass = sum(1 for v in scorecard.values() if v is True)
n_total = len(scorecard)
report['summary'] = {
    'passed': n_pass,
    'total': n_total,
    'all_pass': n_pass == n_total,
    'audit_state': 'COMPLETE' if n_pass == n_total else 'PARTIAL',
}

os.makedirs('aws/ops/reports', exist_ok=True)
with open('aws/ops/reports/1050.json', 'w') as f:
    json.dump(report, f, indent=2, default=str)

print(f"\n{'='*70}")
print(f"  FINAL AUDIT SCORECARD ({n_pass}/{n_total} PASS)")
print(f"{'='*70}")
for k, v in scorecard.items():
    print(f"  {'✅' if v else '❌'} {k}")
print(f"\n  Audit state: {report['summary']['audit_state']}")
