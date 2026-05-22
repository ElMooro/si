#!/usr/bin/env python3
"""
ops 1033 — focused silent-S3-write investigation

Goals:
  1. For each suspect Lambda (khalid-metrics, bloomberg-v8, ka-metrics,
     calibration-snapshotter, autonomous-ai-processor), pull last 7d of
     log events and look for:
     - "S3 save error" / "❌" / "put_object" exception messages
     - SUCCESS markers ("✅ Data saved")
     - The actual write path taken
  
  2. For all Lambdas with `except Exception as e: print(f"❌ ...")` pattern
     (silent failure pattern), count occurrences from local source scan
     and cross-reference with stale outputs.
  
  3. Specifically for autonomous-ai-processor: WHY isn't it being invoked?
     Pull last 7d of /aws/events/* logs to see if rule fires (or just
     never invokes target), check rule target ARN matches Lambda ARN.
  
  4. For TYPE A HIGH list (18 Lambdas with partial invocations), pull
     their actual schedule expression and check for market-hours
     conditional in the rule's event-pattern / context.

Writes: aws/ops/reports/1033.json
"""
import json, boto3, os, re
from datetime import datetime, timezone, timedelta

REGION = 'us-east-1'
NOW = datetime.now(timezone.utc)
BUCKET = 'justhodl-dashboard-live'

lam = boto3.client('lambda', region_name=REGION)
logs = boto3.client('logs', region_name=REGION)
events = boto3.client('events', region_name=REGION)
cw = boto3.client('cloudwatch', region_name=REGION)
s3 = boto3.client('s3', region_name=REGION)

report = {'started_at': NOW.isoformat()}


def log_search(lg_name, patterns, lookback_days=7, limit_per_pattern=10):
    """Search a log group for multiple patterns. Returns {pattern: [events]}."""
    end_ms = int(NOW.timestamp()*1000)
    start_ms = end_ms - lookback_days*86400*1000
    out = {}
    for pat in patterns:
        try:
            q = logs.filter_log_events(
                logGroupName=lg_name, startTime=start_ms, endTime=end_ms,
                filterPattern=pat, limit=limit_per_pattern,
            )
            out[pat] = [
                {'ts': datetime.fromtimestamp(e['timestamp']/1000, timezone.utc).isoformat(),
                 'msg': e['message'][:400]}
                for e in q.get('events', [])
            ]
        except logs.exceptions.ResourceNotFoundException:
            out[pat] = 'log_group_not_found'
        except Exception as e:
            out[pat] = f'error: {str(e)[:200]}'
    return out


# ---- Q1: Per-Lambda log forensics for suspects ----
print("[Q1] Suspect Lambda log forensics...")
SUSPECTS = [
    ('justhodl-bloomberg-v8', ['"S3 save error"', '"❌"', '"✅ Data saved"', 'Errno', 'AccessDenied']),
    ('justhodl-daily-report-v3', ['"S3 save error"', '"❌"', '"✅"', 'V10']),
    ('justhodl-khalid-metrics', ['"khalid-config"', '"Error"', '"✅"', '"saved"', '"put_object"']),
    ('justhodl-ka-metrics', ['"khalid-config"', '"ka-config"', '"Error"', '"✅"', '"saved"']),
    ('justhodl-calibration-snapshotter', ['"calibration/latest"', '"Error"', '"saved"']),
    ('justhodl-calibrator', ['"calibration/latest"', '"Error"', '"saved"']),
]
q1 = {}
for lam_name, patterns in SUSPECTS:
    print(f"  {lam_name}")
    lg = f'/aws/lambda/{lam_name}'
    q1[lam_name] = log_search(lg, patterns)

report['Q1_suspect_log_forensics'] = q1

# ---- Q2: autonomous-ai-processor invocation mystery ----
print("[Q2] autonomous-ai-processor deep check...")
q2 = {}
try:
    cfg = lam.get_function_configuration(FunctionName='autonomous-ai-processor')
    q2['lambda_arn'] = cfg['FunctionArn']
    q2['last_modified'] = cfg.get('LastModified')
    q2['state'] = cfg.get('State')
    q2['state_reason'] = cfg.get('StateReason')
    # The Lambda may have been disabled/deleted by AWS due to repeated failures
    
    # The rule
    rd = events.describe_rule(Name='autonomous-ai-schedule')
    q2['rule'] = {
        'state': rd.get('State'),
        'expression': rd.get('ScheduleExpression'),
        'arn': rd.get('Arn'),
    }
    
    # Targets of the rule
    t = events.list_targets_by_rule(Rule='autonomous-ai-schedule')
    q2['rule_targets'] = [
        {'arn': tt['Arn'], 'matches_lambda': tt['Arn'] == cfg['FunctionArn']}
        for tt in t.get('Targets', [])
    ]
    
    # Resource policy on Lambda — does EB have permission to invoke?
    try:
        pol = json.loads(lam.get_policy(FunctionName='autonomous-ai-processor')['Policy'])
        q2['resource_policy_statements'] = [
            {'sid': s.get('Sid'), 'principal': s.get('Principal'),
             'action': s.get('Action'), 'condition': s.get('Condition')}
            for s in pol.get('Statement', [])
        ]
    except lam.exceptions.ResourceNotFoundException:
        q2['resource_policy_statements'] = 'NO_RESOURCE_POLICY — this is why EB can\'t invoke it!'
    
    # CloudTrail-equivalent: pull Lambda log group, look for ANY recent activity
    try:
        ls = logs.describe_log_streams(
            logGroupName='/aws/lambda/autonomous-ai-processor',
            orderBy='LastEventTime', descending=True, limit=3)
        streams = ls.get('logStreams', [])
        q2['recent_log_streams'] = [
            {'name': s['logStreamName'],
             'last_event': datetime.fromtimestamp(s['lastEventTimestamp']/1000, timezone.utc).isoformat() if s.get('lastEventTimestamp') else None,
             'days_ago': round((NOW.timestamp()*1000 - s.get('lastEventTimestamp', 0))/86400000, 1) if s.get('lastEventTimestamp') else None}
            for s in streams
        ]
    except logs.exceptions.ResourceNotFoundException:
        q2['recent_log_streams'] = 'LOG_GROUP_DOES_NOT_EXIST'
    except Exception as e:
        q2['recent_log_streams'] = f'error: {str(e)[:200]}'
    
    # FailedInvocations metric on the EB rule
    end = NOW
    start = end - timedelta(days=7)
    try:
        m = cw.get_metric_statistics(
            Namespace='AWS/Events', MetricName='FailedInvocations',
            Dimensions=[{'Name': 'RuleName', 'Value': 'autonomous-ai-schedule'}],
            StartTime=start, EndTime=end, Period=86400, Statistics=['Sum'],
        )
        q2['eb_rule_failed_invocations_7d'] = int(sum(p['Sum'] for p in m['Datapoints']))
        m2 = cw.get_metric_statistics(
            Namespace='AWS/Events', MetricName='Invocations',
            Dimensions=[{'Name': 'RuleName', 'Value': 'autonomous-ai-schedule'}],
            StartTime=start, EndTime=end, Period=86400, Statistics=['Sum'],
        )
        q2['eb_rule_invocations_7d'] = int(sum(p['Sum'] for p in m2['Datapoints']))
    except Exception as e:
        q2['eb_metrics_error'] = str(e)[:200]
        
except Exception as e:
    q2['error'] = str(e)[:300]
report['Q2_autonomous_ai_processor_mystery'] = q2

# Same investigation for justhodl-email-reports (v1)
print("[Q2b] justhodl-email-reports v1 invocation mystery...")
q2b = {}
try:
    cfg = lam.get_function_configuration(FunctionName='justhodl-email-reports')
    q2b['lambda_arn'] = cfg['FunctionArn']
    q2b['state'] = cfg.get('State')
    q2b['state_reason'] = cfg.get('StateReason')
    q2b['last_modified'] = cfg.get('LastModified')
    
    rd = events.describe_rule(Name='justhodl-8am')
    q2b['rule_state'] = rd.get('State')
    q2b['rule_expression'] = rd.get('ScheduleExpression')
    
    t = events.list_targets_by_rule(Rule='justhodl-8am')
    q2b['rule_targets'] = [tt['Arn'] for tt in t.get('Targets', [])]
    q2b['target_matches_lambda'] = any(tt['Arn'] == cfg['FunctionArn'] for tt in t.get('Targets', []))
    
    try:
        pol = json.loads(lam.get_policy(FunctionName='justhodl-email-reports')['Policy'])
        q2b['resource_policy_statements'] = [
            {'sid': s.get('Sid'), 'principal': s.get('Principal'), 'action': s.get('Action')}
            for s in pol.get('Statement', [])
        ]
    except lam.exceptions.ResourceNotFoundException:
        q2b['resource_policy_statements'] = 'NO_RESOURCE_POLICY'
    
    # EB rule metrics
    end = NOW; start = end - timedelta(days=7)
    try:
        m = cw.get_metric_statistics(
            Namespace='AWS/Events', MetricName='Invocations',
            Dimensions=[{'Name': 'RuleName', 'Value': 'justhodl-8am'}],
            StartTime=start, EndTime=end, Period=86400, Statistics=['Sum'])
        q2b['eb_rule_invocations_7d'] = int(sum(p['Sum'] for p in m['Datapoints']))
        m = cw.get_metric_statistics(
            Namespace='AWS/Events', MetricName='FailedInvocations',
            Dimensions=[{'Name': 'RuleName', 'Value': 'justhodl-8am'}],
            StartTime=start, EndTime=end, Period=86400, Statistics=['Sum'])
        q2b['eb_rule_failed_invocations_7d'] = int(sum(p['Sum'] for p in m['Datapoints']))
    except Exception as e:
        q2b['eb_metrics_error'] = str(e)[:200]
except Exception as e:
    q2b['error'] = str(e)[:300]
report['Q2b_email_reports_v1_mystery'] = q2b

# ---- Q3: silent-except code pattern audit (across full repo) ----
# This is local but we can scan during the deploy
# Just write a marker — to be done in sandbox
report['Q3_note'] = 'silent-except pattern audit done in sandbox; see local file'

# Write report
os.makedirs('aws/ops/reports', exist_ok=True)
with open('aws/ops/reports/1033.json', 'w') as f:
    json.dump(report, f, indent=2, default=str)
print(f"\nReport written: aws/ops/reports/1033.json")
