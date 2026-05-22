#!/usr/bin/env python3
"""
ops 1029 — LIVE AWS A-to-Z AUDIT (institutional-grade)
======================================================
Runs on AWS via run-ops.yml. Hits CloudWatch / Lambda / EventBridge / S3
for facts the sandbox can't reach. Pairs with the local code-truth audit
to produce a complete, hedge-fund-grade audit report.

Sections:
  A. Lambda inventory: runtime, memory, timeout, DLQ, X-Ray, ReservedConcurrency,
     LastModified, Architecture, env-var key names (not values), code SHA.
  B. EventBridge schedules: all rules+targets, enabled state, expression.
  C. S3 freshness: every data/* key + age in hours, by family prefix.
  D. CloudWatch: per-Lambda invocation count last 7d, errors, throttles, p99 duration.
  E. Alarms: total alarms by namespace; Lambdas with zero alarms (institutional gap).
  F. Log groups: which Lambdas have NO recent log streams (orphan/dead).
  G. Costs hint: by-family invocation counts to spot expensive families.
"""
import json, os, boto3, time
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

REGION = 'us-east-1'
BUCKET = 'justhodl-dashboard-live'
NOW = datetime.now(timezone.utc)

lam = boto3.client('lambda', region_name=REGION)
events = boto3.client('events', region_name=REGION)
scheduler = boto3.client('scheduler', region_name=REGION)
s3 = boto3.client('s3', region_name=REGION)
cw = boto3.client('cloudwatch', region_name=REGION)
logs = boto3.client('logs', region_name=REGION)

report = {
    'started_at': NOW.isoformat(),
    'A_lambdas': {},
    'B_schedules': {'eventbridge_rules': [], 'scheduler_v2': []},
    'C_s3_freshness': {'by_prefix': {}, 'stale_24h': [], 'stale_72h': []},
    'D_cloudwatch_7d': {},
    'E_alarms': {'total': 0, 'lambdas_with_zero_alarms': []},
    'F_dead_log_groups': [],
    'G_cost_hint': {'by_family': {}},
    'summary': {},
}

# -------- A: Lambda inventory --------
print("[A] Enumerating Lambdas...")
lambdas = []
paginator = lam.get_paginator('list_functions')
for page in paginator.paginate():
    lambdas.extend(page['Functions'])
print(f"  found {len(lambdas)} functions")

def get_fn_detail(fn):
    name = fn['FunctionName']
    try:
        cfg = lam.get_function_configuration(FunctionName=name)
        env_keys = sorted(list((cfg.get('Environment', {}) or {}).get('Variables', {}).keys()))
        try:
            cc = lam.get_function_concurrency(FunctionName=name)
            reserved = cc.get('ReservedConcurrentExecutions')
        except Exception:
            reserved = None
        return name, {
            'runtime': cfg.get('Runtime'),
            'memory': cfg.get('MemorySize'),
            'timeout': cfg.get('Timeout'),
            'arch': cfg.get('Architectures', ['x86_64'])[0],
            'last_modified': cfg.get('LastModified'),
            'code_sha': cfg.get('CodeSha256', '')[:12],
            'code_size': cfg.get('CodeSize'),
            'dlq': bool((cfg.get('DeadLetterConfig') or {}).get('TargetArn')),
            'xray': cfg.get('TracingConfig', {}).get('Mode') == 'Active',
            'reserved_concurrency': reserved,
            'env_var_keys': env_keys,
            'has_vpc': bool(cfg.get('VpcConfig', {}).get('VpcId')),
            'role': cfg.get('Role', '').split('/')[-1],
            'has_layers': len(cfg.get('Layers', [])) > 0,
        }
    except Exception as e:
        return name, {'error': str(e)[:200]}

with ThreadPoolExecutor(max_workers=16) as ex:
    futures = [ex.submit(get_fn_detail, fn) for fn in lambdas]
    for fut in as_completed(futures):
        name, det = fut.result()
        report['A_lambdas'][name] = det

# -------- B: EventBridge --------
print("[B] EventBridge rules + Scheduler...")
try:
    rules = []
    paginator = events.get_paginator('list_rules')
    for page in paginator.paginate():
        for r in page['Rules']:
            targets = []
            try:
                t = events.list_targets_by_rule(Rule=r['Name'])
                targets = [tt['Arn'].split(':')[-1] for tt in t.get('Targets', [])]
            except Exception: pass
            rules.append({
                'name': r['Name'],
                'state': r.get('State'),
                'schedule': r.get('ScheduleExpression', ''),
                'targets': targets,
            })
    report['B_schedules']['eventbridge_rules'] = rules
except Exception as e:
    report['B_schedules']['eventbridge_error'] = str(e)[:200]

try:
    scheds = []
    paginator = scheduler.get_paginator('list_schedules')
    for page in paginator.paginate():
        for s in page['Schedules']:
            scheds.append({
                'name': s['Name'],
                'state': s.get('State'),
                'group': s.get('GroupName'),
                'target': (s.get('Target') or {}).get('Arn', '').split(':')[-1],
            })
    report['B_schedules']['scheduler_v2'] = scheds
except Exception as e:
    report['B_schedules']['scheduler_error'] = str(e)[:200]

# -------- C: S3 freshness --------
print("[C] S3 freshness scan...")
all_keys = []
paginator = s3.get_paginator('list_objects_v2')
for page in paginator.paginate(Bucket=BUCKET, Prefix='data/'):
    for obj in page.get('Contents', []):
        all_keys.append({
            'key': obj['Key'],
            'age_h': round((NOW - obj['LastModified']).total_seconds() / 3600, 1),
            'size': obj['Size'],
        })
print(f"  scanned {len(all_keys)} keys")

# By-prefix freshness
from collections import defaultdict
by_prefix = defaultdict(list)
for k in all_keys:
    parts = k['key'].split('/')
    # data/foo.json → foo, data/sub/x.json → data/sub
    prefix = parts[1] if len(parts) > 2 else 'root'
    by_prefix[prefix].append(k['age_h'])

for prefix, ages in by_prefix.items():
    report['C_s3_freshness']['by_prefix'][prefix] = {
        'count': len(ages),
        'min_age_h': round(min(ages), 1),
        'max_age_h': round(max(ages), 1),
        'median_age_h': round(sorted(ages)[len(ages)//2], 1),
    }

# Stale lists (exclude obvious archive paths)
def is_archive(k):
    return '/archive/' in k or '/_archive/' in k or '/snapshots/' in k or '/history/' in k

for k in all_keys:
    if k['age_h'] > 24 and not is_archive(k['key']):
        report['C_s3_freshness']['stale_24h'].append(k)
    if k['age_h'] > 72 and not is_archive(k['key']):
        report['C_s3_freshness']['stale_72h'].append(k)

report['C_s3_freshness']['stale_24h'] = sorted(report['C_s3_freshness']['stale_24h'], key=lambda x: -x['age_h'])[:50]
report['C_s3_freshness']['stale_72h'] = sorted(report['C_s3_freshness']['stale_72h'], key=lambda x: -x['age_h'])[:50]

# -------- D: CloudWatch metrics (last 7d invocations / errors / throttles) --------
print("[D] CloudWatch 7d metrics...")
end = NOW
start = end - timedelta(days=7)

def get_metric_sum(metric, fn_name):
    try:
        resp = cw.get_metric_statistics(
            Namespace='AWS/Lambda', MetricName=metric,
            Dimensions=[{'Name': 'FunctionName', 'Value': fn_name}],
            StartTime=start, EndTime=end, Period=86400, Statistics=['Sum'],
        )
        return int(sum(p['Sum'] for p in resp['Datapoints']))
    except Exception:
        return None

def get_metric_p99(fn_name):
    try:
        resp = cw.get_metric_statistics(
            Namespace='AWS/Lambda', MetricName='Duration',
            Dimensions=[{'Name': 'FunctionName', 'Value': fn_name}],
            StartTime=start, EndTime=end, Period=86400, ExtendedStatistics=['p99'],
        )
        if not resp['Datapoints']: return None
        return round(max(p['ExtendedStatistics']['p99'] for p in resp['Datapoints']), 1)
    except Exception:
        return None

def get_cw_bundle(name):
    return name, {
        'invocations_7d': get_metric_sum('Invocations', name),
        'errors_7d': get_metric_sum('Errors', name),
        'throttles_7d': get_metric_sum('Throttles', name),
        'p99_duration_ms': get_metric_p99(name),
    }

with ThreadPoolExecutor(max_workers=12) as ex:
    futures = [ex.submit(get_cw_bundle, fn['FunctionName']) for fn in lambdas]
    for fut in as_completed(futures):
        name, det = fut.result()
        report['D_cloudwatch_7d'][name] = det

# -------- E: Alarms --------
print("[E] CloudWatch alarms...")
all_alarms = []
paginator = cw.get_paginator('describe_alarms')
for page in paginator.paginate():
    for a in page.get('MetricAlarms', []):
        all_alarms.append({
            'name': a['AlarmName'],
            'metric': a.get('MetricName'),
            'namespace': a.get('Namespace'),
            'dims': [(d['Name'], d['Value']) for d in a.get('Dimensions', [])],
            'state': a.get('StateValue'),
        })
report['E_alarms']['total'] = len(all_alarms)
# Map alarms to Lambda
lambdas_with_alarm = set()
for a in all_alarms:
    for d_name, d_val in a['dims']:
        if d_name == 'FunctionName':
            lambdas_with_alarm.add(d_val)
report['E_alarms']['lambdas_with_alarm_count'] = len(lambdas_with_alarm)
report['E_alarms']['lambdas_with_zero_alarms'] = sorted([
    fn['FunctionName'] for fn in lambdas if fn['FunctionName'] not in lambdas_with_alarm
])

# -------- F: dead log groups (no recent activity) --------
print("[F] Log groups inactive 30d+...")
def check_log_recency(fn_name):
    lg = f'/aws/lambda/{fn_name}'
    try:
        resp = logs.describe_log_streams(
            logGroupName=lg, orderBy='LastEventTime', descending=True, limit=1,
        )
        if not resp.get('logStreams'):
            return fn_name, None
        last = resp['logStreams'][0].get('lastEventTimestamp')
        if last is None: return fn_name, None
        age_d = (NOW.timestamp()*1000 - last) / 86400000
        return fn_name, round(age_d, 1)
    except Exception:
        return fn_name, 'no_log_group'

with ThreadPoolExecutor(max_workers=16) as ex:
    futures = [ex.submit(check_log_recency, fn['FunctionName']) for fn in lambdas]
    for fut in as_completed(futures):
        name, age = fut.result()
        if age == 'no_log_group' or age is None or (isinstance(age, float) and age > 30):
            report['F_dead_log_groups'].append({'lambda': name, 'last_log_days_ago': age})

# -------- G: Cost hint by family --------
print("[G] Cost hint by family...")
from collections import defaultdict
fam_invocations = defaultdict(int)
for name, cw_data in report['D_cloudwatch_7d'].items():
    # Crude family from name segment after "justhodl-"
    n = name.replace('justhodl-', '')
    fam = n.split('-')[0]
    fam_invocations[fam] += (cw_data.get('invocations_7d') or 0)

report['G_cost_hint']['by_family'] = dict(sorted(fam_invocations.items(), key=lambda x: -x[1])[:30])

# -------- Summary --------
print("[Summary]")
report['summary'] = {
    'n_lambdas': len(lambdas),
    'n_lambdas_with_dlq': sum(1 for d in report['A_lambdas'].values() if isinstance(d, dict) and d.get('dlq')),
    'n_lambdas_with_xray': sum(1 for d in report['A_lambdas'].values() if isinstance(d, dict) and d.get('xray')),
    'n_lambdas_with_reserved_concurrency': sum(1 for d in report['A_lambdas'].values() if isinstance(d, dict) and d.get('reserved_concurrency') is not None),
    'n_eventbridge_rules': len(report['B_schedules']['eventbridge_rules']),
    'n_scheduler_v2': len(report['B_schedules']['scheduler_v2']),
    'n_s3_keys': len(all_keys),
    'n_stale_24h_keys': len([k for k in all_keys if k['age_h']>24 and not is_archive(k['key'])]),
    'n_stale_72h_keys': len([k for k in all_keys if k['age_h']>72 and not is_archive(k['key'])]),
    'n_alarms_total': len(all_alarms),
    'n_lambdas_with_alarms': len(lambdas_with_alarm),
    'n_lambdas_zero_alarms': len(report['E_alarms']['lambdas_with_zero_alarms']),
    'n_lambdas_no_recent_logs_30d': len(report['F_dead_log_groups']),
    'total_invocations_7d': sum((d.get('invocations_7d') or 0) for d in report['D_cloudwatch_7d'].values()),
    'total_errors_7d': sum((d.get('errors_7d') or 0) for d in report['D_cloudwatch_7d'].values()),
    'total_throttles_7d': sum((d.get('throttles_7d') or 0) for d in report['D_cloudwatch_7d'].values()),
}

# Write report
report['completed_at'] = datetime.now(timezone.utc).isoformat()
out = '/tmp/1029_live_audit.json'
with open(out, 'w') as f:
    json.dump(report, f, indent=2, default=str)

# Upload final report to S3 + return summary to GH Actions
s3.put_object(
    Bucket=BUCKET,
    Key='ops/reports/1029_live_audit.json',
    Body=json.dumps(report, default=str).encode(),
    ContentType='application/json',
)

print(json.dumps(report['summary'], indent=2))
print(f"\nFull report uploaded to s3://{BUCKET}/ops/reports/1029_live_audit.json")

# Also save in repo path expected by run-ops.yml
os.makedirs('aws/ops/reports', exist_ok=True)
with open('aws/ops/reports/1029.json', 'w') as f:
    json.dump(report, f, indent=2, default=str)
print(f"Local report: aws/ops/reports/1029.json")
