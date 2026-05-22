#!/usr/bin/env python3
"""
ops 1031 — ROUND-3 DEEP VERIFICATION

Targets all remaining doubts from ops 1029/1030:
  Q1. portfolio-catalysts: CW says 33/43 errors but 0 ERROR log lines found.
      → Pull ALL recent log events (not filtered), check actual outcomes by request.
  Q2. R-series Lambdas: are the schedules NEW (created today) or pre-existing?
      → Check Scheduler v2 schedule creation_date.
  Q3. "Zombie" EB rules — are they ENABLED or DISABLED?
      → Get each rule state.
  Q4. khalid-config.json 84 days stale — who's the real writer? Schedule check.
  Q5. Lambda code drift: AWS deployed code SHA vs git HEAD repo for same name?
      → Build matrix.
  Q6. Lambda permission errors / Scheduler v2 invoke permission for R-series:
      → Has Scheduler v2 been granted invoke permission?
  Q7. Real freshness of data/report.json — who's winning the race?
      → Pull S3 metadata + ETag history if available.
"""
import json, boto3, os, base64
from datetime import datetime, timezone, timedelta

REGION = 'us-east-1'
NOW = datetime.now(timezone.utc)
BUCKET = 'justhodl-dashboard-live'

lam = boto3.client('lambda', region_name=REGION)
logs = boto3.client('logs', region_name=REGION)
cw = boto3.client('cloudwatch', region_name=REGION)
events = boto3.client('events', region_name=REGION)
sched = boto3.client('scheduler', region_name=REGION)
s3 = boto3.client('s3', region_name=REGION)

report = {'started_at': NOW.isoformat()}

# ---- Q1: portfolio-catalysts deep log dive ----
print("[Q1] portfolio-catalysts full log inspection...")
lg = '/aws/lambda/justhodl-portfolio-catalysts'
q1 = {'log_group': lg}
try:
    # All log events past 7 days (no filter)
    end_ms = int(NOW.timestamp()*1000)
    start_ms = end_ms - 7*86400*1000
    
    streams = logs.describe_log_streams(
        logGroupName=lg, orderBy='LastEventTime', descending=True, limit=20
    ).get('logStreams', [])
    q1['n_recent_streams'] = len(streams)
    q1['streams_recent'] = []
    
    # Pull last 50 events from the most recent stream
    if streams:
        s = streams[0]
        last_evt = s.get('lastEventTimestamp')
        q1['streams_recent'].append({
            'name': s['logStreamName'],
            'last_event': datetime.fromtimestamp(last_evt/1000, timezone.utc).isoformat() if last_evt else None,
            'stored_bytes': s.get('storedBytes', 0),
        })
        # All events
        evts = logs.get_log_events(
            logGroupName=lg, logStreamName=s['logStreamName'], limit=100, startFromHead=False
        ).get('events', [])
        # Sample event types
        q1['sample_event_messages'] = [
            {'ts': datetime.fromtimestamp(e['timestamp']/1000, timezone.utc).isoformat(),
             'msg': e['message'][:300]}
            for e in evts[-30:]
        ]
        
        # Count REPORT lines vs everything
        report_lines = [e for e in evts if e['message'].startswith('REPORT')]
        init_lines = [e for e in evts if 'Init Duration' in e['message']]
        error_lines = [e for e in evts if 'ERROR' in e['message'] or 'Exception' in e['message']]
        timeout_lines = [e for e in evts if 'Task timed out' in e['message']]
        q1['line_counts'] = {
            'REPORT': len(report_lines),
            'Init Duration': len(init_lines),
            'ERROR_or_Exception': len(error_lines),
            'Task_timed_out': len(timeout_lines),
            'total_in_sample': len(evts),
        }
        # Extract billed duration / max memory from REPORT lines
        import re
        durations = []
        for e in report_lines:
            m = re.search(r'Billed Duration:\s*(\d+)\s*ms', e['message'])
            if m: durations.append(int(m.group(1)))
        if durations:
            durations.sort()
            q1['billed_duration_ms'] = {
                'min': durations[0], 'max': durations[-1],
                'median': durations[len(durations)//2],
                'count': len(durations),
            }
    
    # Filter for any of: ERROR, Exception, Timeout, ImportError, init_error
    for pattern in ['Task timed out', 'errorMessage', 'errorType', 'Process exited', 'Runtime.HandlerNotFound']:
        try:
            q = logs.filter_log_events(
                logGroupName=lg, startTime=start_ms, endTime=end_ms,
                filterPattern=f'"{pattern}"', limit=5,
            )
            hits = q.get('events', [])
            if hits:
                q1.setdefault('special_patterns', {})[pattern] = [
                    {'ts': datetime.fromtimestamp(e['timestamp']/1000, timezone.utc).isoformat(),
                     'msg': e['message'][:300]}
                    for e in hits[:3]
                ]
        except Exception: pass
    
    # CW metric breakdown — invocations vs errors at higher resolution
    end = NOW
    start = end - timedelta(days=7)
    inv = cw.get_metric_statistics(
        Namespace='AWS/Lambda', MetricName='Invocations',
        Dimensions=[{'Name': 'FunctionName', 'Value': 'justhodl-portfolio-catalysts'}],
        StartTime=start, EndTime=end, Period=3600, Statistics=['Sum'],
    )
    err = cw.get_metric_statistics(
        Namespace='AWS/Lambda', MetricName='Errors',
        Dimensions=[{'Name': 'FunctionName', 'Value': 'justhodl-portfolio-catalysts'}],
        StartTime=start, EndTime=end, Period=3600, Statistics=['Sum'],
    )
    # Daily summary
    daily_inv = {}
    daily_err = {}
    for p in inv['Datapoints']:
        d = p['Timestamp'].date().isoformat()
        daily_inv[d] = daily_inv.get(d, 0) + p['Sum']
    for p in err['Datapoints']:
        d = p['Timestamp'].date().isoformat()
        daily_err[d] = daily_err.get(d, 0) + p['Sum']
    q1['daily_inv_vs_err'] = {
        d: {'inv': int(daily_inv.get(d, 0)), 'err': int(daily_err.get(d, 0))}
        for d in sorted(set(daily_inv) | set(daily_err))
    }
except Exception as e:
    q1['error'] = str(e)[:300]
report['Q1_portfolio_catalysts'] = q1

# ---- Q2: R-series schedule creation dates ----
print("[Q2] R-series schedule creation_date...")
q2 = {}
R_SERIES = [
    'justhodl-sec-filing-diff', 'justhodl-transcript-indexer', 'justhodl-transcript-query',
    'justhodl-peer-comparison', 'justhodl-screen-builder', 'justhodl-fedwatch-rate-probability',
    'justhodl-supply-chain-linkage', 'justhodl-cftc-deep-view',
    'justhodl-fx-decomposition', 'justhodl-factor-decomposition',
]
# Build a map of schedule_name → target Lambda
all_schedules = []
paginator = sched.get_paginator('list_schedules')
for page in paginator.paginate():
    all_schedules.extend(page.get('Schedules', []))

for r in R_SERIES:
    matched = []
    for s in all_schedules:
        tgt = (s.get('Target') or {}).get('Arn', '')
        if r in tgt:
            try:
                d = sched.get_schedule(Name=s['Name'], GroupName=s.get('GroupName', 'default'))
                matched.append({
                    'name': s['Name'],
                    'state': d.get('State'),
                    'expression': d.get('ScheduleExpression'),
                    'creation_date': d.get('CreationDate').isoformat() if d.get('CreationDate') else None,
                    'last_modification_date': d.get('LastModificationDate').isoformat() if d.get('LastModificationDate') else None,
                    'role_arn_present': bool(d.get('Target', {}).get('RoleArn')),
                })
            except Exception as e:
                matched.append({'name': s['Name'], 'error': str(e)[:200]})
    # Also Lambda last_modified
    try:
        cfg = lam.get_function_configuration(FunctionName=r)
        lambda_last_mod = cfg.get('LastModified')
    except Exception:
        lambda_last_mod = None
    q2[r] = {
        'lambda_last_modified': lambda_last_mod,
        'schedules': matched,
    }
report['Q2_r_series_schedule_history'] = q2

# ---- Q3: zombie EB rule states ----
print("[Q3] Zombie EB rule states...")
q3 = {}
for name in ['macro-report-api', 'autonomous-ai-processor', 'justhodl-email-reports', 'justhodl-daily-macro-report']:
    try:
        cfg = lam.get_function_configuration(FunctionName=name)
        fn_arn = cfg['FunctionArn']
        # Find EB rules targeting this Lambda
        rule_names = []
        paginator = events.get_paginator('list_rule_names_by_target')
        for page in paginator.paginate(TargetArn=fn_arn):
            rule_names.extend(page.get('RuleNames', []))
        details = []
        for rn in rule_names:
            try:
                rd = events.describe_rule(Name=rn)
                details.append({
                    'name': rn,
                    'state': rd.get('State'),
                    'schedule': rd.get('ScheduleExpression'),
                    'event_pattern': rd.get('EventPattern'),
                })
            except Exception: pass
        # Scheduler v2 too
        sv2 = []
        for s in all_schedules:
            if name in (s.get('Target') or {}).get('Arn', ''):
                try:
                    d = sched.get_schedule(Name=s['Name'], GroupName=s.get('GroupName','default'))
                    sv2.append({'name': s['Name'], 'state': d.get('State'),
                                'expression': d.get('ScheduleExpression')})
                except Exception: pass
        q3[name] = {'eb_rules': details, 'scheduler_v2': sv2,
                    'function_last_modified': cfg.get('LastModified')}
    except Exception as e:
        q3[name] = {'error': str(e)[:200]}
report['Q3_zombie_rule_states'] = q3

# ---- Q4: khalid-config.json freshness mystery ----
print("[Q4] khalid-config.json: who actually writes?")
q4 = {}
try:
    head = s3.head_object(Bucket=BUCKET, Key='data/khalid-config.json')
    q4['last_modified'] = head['LastModified'].isoformat()
    q4['age_h'] = round((NOW - head['LastModified']).total_seconds()/3600, 1)
    q4['size'] = head['ContentLength']
    q4['etag'] = head.get('ETag')
    q4['metadata'] = head.get('Metadata', {})
except Exception as e:
    q4['head_error'] = str(e)[:200]

# Also check data/ka-config.json (the dual-write target from ka-metrics)
try:
    head2 = s3.head_object(Bucket=BUCKET, Key='data/ka-config.json')
    q4['ka_config_last_modified'] = head2['LastModified'].isoformat()
    q4['ka_config_age_h'] = round((NOW - head2['LastModified']).total_seconds()/3600, 1)
except Exception as e:
    q4['ka_config_head_error'] = str(e)[:200]

# Schedule check for ka-metrics and khalid-metrics
for n in ['justhodl-ka-metrics', 'justhodl-khalid-metrics']:
    try:
        cfg = lam.get_function_configuration(FunctionName=n)
        fn_arn = cfg['FunctionArn']
        # EB rules
        rn_list = []
        paginator = events.get_paginator('list_rule_names_by_target')
        for page in paginator.paginate(TargetArn=fn_arn):
            rn_list.extend(page.get('RuleNames', []))
        eb_states = []
        for rn in rn_list:
            try:
                rd = events.describe_rule(Name=rn)
                eb_states.append({'name': rn, 'state': rd.get('State'), 'schedule': rd.get('ScheduleExpression')})
            except Exception: pass
        # SV2
        sv2 = []
        for s in all_schedules:
            if n in (s.get('Target') or {}).get('Arn', ''):
                try:
                    d = sched.get_schedule(Name=s['Name'], GroupName=s.get('GroupName','default'))
                    sv2.append({'name': s['Name'], 'state': d.get('State'),
                                'expression': d.get('ScheduleExpression')})
                except Exception: pass
        # CW invocations 30d
        end = NOW
        start = end - timedelta(days=30)
        m = cw.get_metric_statistics(
            Namespace='AWS/Lambda', MetricName='Invocations',
            Dimensions=[{'Name': 'FunctionName', 'Value': n}],
            StartTime=start, EndTime=end, Period=86400, Statistics=['Sum'],
        )
        inv30 = int(sum(p['Sum'] for p in m['Datapoints']))
        q4[n] = {
            'last_modified': cfg.get('LastModified'),
            'eb_rules': eb_states,
            'scheduler_v2': sv2,
            'invocations_30d': inv30,
        }
    except Exception as e:
        q4[n] = {'error': str(e)[:200]}
report['Q4_khalid_config'] = q4

# ---- Q5: Race-condition winner: who wrote data/report.json most recently? ----
print("[Q5] data/report.json race: winner...")
q5 = {}
try:
    head = s3.head_object(Bucket=BUCKET, Key='data/report.json')
    q5['key'] = 'data/report.json'
    q5['last_modified'] = head['LastModified'].isoformat()
    q5['age_h'] = round((NOW - head['LastModified']).total_seconds()/3600, 1)
    q5['size'] = head['ContentLength']
    
    # Object versioning ? List versions
    try:
        v = s3.list_object_versions(Bucket=BUCKET, Prefix='data/report.json', MaxKeys=5)
        q5['versions'] = [
            {'version': vv.get('VersionId'), 'last_modified': vv['LastModified'].isoformat(), 'size': vv.get('Size')}
            for vv in v.get('Versions', [])[:5]
        ]
    except Exception: pass
    
    # Get last 100 lines of content to see which Lambda's signature wrote it
    obj = s3.get_object(Bucket=BUCKET, Key='data/report.json')
    body = obj['Body'].read(2000).decode('utf-8', errors='replace')
    q5['first_2k_chars'] = body[:2000]
except Exception as e:
    q5['error'] = str(e)[:300]

# Invocation timing for both writers (when did each run last)
for n in ['justhodl-bloomberg-v8', 'justhodl-daily-report-v3']:
    try:
        end = NOW
        start = end - timedelta(days=2)
        m = cw.get_metric_statistics(
            Namespace='AWS/Lambda', MetricName='Invocations',
            Dimensions=[{'Name': 'FunctionName', 'Value': n}],
            StartTime=start, EndTime=end, Period=3600, Statistics=['Sum'],
        )
        # Find most recent non-zero hour
        dps = sorted(m['Datapoints'], key=lambda p: p['Timestamp'], reverse=True)
        last = None
        for p in dps:
            if p['Sum'] > 0:
                last = {'hour': p['Timestamp'].isoformat(), 'invocations': int(p['Sum'])}
                break
        q5[f'last_invocation_{n}'] = last
    except Exception as e:
        q5[f'error_{n}'] = str(e)[:200]
report['Q5_race_winner'] = q5

# ---- Q6: Scheduler v2 invoke permission for R-series ----
print("[Q6] Scheduler v2 invoke permission check...")
q6 = {}
for n in R_SERIES:
    try:
        pol_resp = lam.get_policy(FunctionName=n)
        pol = json.loads(pol_resp['Policy'])
        sched_statements = [
            st for st in pol.get('Statement', [])
            if 'scheduler.amazonaws.com' in str(st.get('Principal', {})) or 'events.amazonaws.com' in str(st.get('Principal', {}))
        ]
        q6[n] = {
            'has_scheduler_perm': any('scheduler' in str(s) for s in sched_statements),
            'has_events_perm': any('events' in str(s) for s in sched_statements),
            'n_statements': len(pol.get('Statement', [])),
            'first_3_principals': [st.get('Principal') for st in pol.get('Statement', [])[:3]],
        }
    except lam.exceptions.ResourceNotFoundException:
        q6[n] = {'error': 'no_resource_based_policy_at_all'}
    except Exception as e:
        q6[n] = {'error': str(e)[:200]}
report['Q6_scheduler_invoke_permission'] = q6

# ---- Q7: Lambda code drift — AWS deployed SHA vs repo HEAD ----
print("[Q7] code drift detection (sample 20 Lambdas)...")
q7 = {}
# We can't access git/repo from inside Lambda exec environment easily.
# But we can check CodeSha256 of deployed code vs the code we last pushed.
# Skip for this round — would need a separate workflow comparing repo SHA → lambda SHA.
q7['note'] = 'skipped — requires repo-side comparison (TODO: add to CI)'
report['Q7_code_drift'] = q7

# Write report
os.makedirs('aws/ops/reports', exist_ok=True)
with open('aws/ops/reports/1031.json', 'w') as f:
    json.dump(report, f, indent=2, default=str)

print("\n=== FULL REPORT WRITTEN TO aws/ops/reports/1031.json ===")
print(f"Size: {os.path.getsize('aws/ops/reports/1031.json'):,} bytes")
