#!/usr/bin/env python3
"""
ops 1032 — COMPREHENSIVE SILENT-FAILURE INVESTIGATION

Detects three classes of silent failure across all 399 Lambdas:

  TYPE A: Schedule fires but Lambda doesn't invoke
          (e.g. autonomous-ai-processor: rate(5min) ENABLED, 0 invocations)
          Compares: expected_invocations_per_7d vs actual CW Invocations 7d.

  TYPE B: Lambda invokes successfully but its output is stale
          (e.g. khalid-metrics: 56 inv/30d, output 84 days stale)
          Compares: actual S3 LastModified vs expected age (2× schedule cadence).

  TYPE C: Lambda errors silently (errors > 0, no alarm = silent)
          Already known: all 86 errors in 7d are silent.

  TYPE D: Asymmetric writers — a key has 2+ producers but only 1 actually
          writes recently (the other appears dead in production despite
          deployed code).

  TYPE E: Lambdas with NO schedule, NO function URL, NO event mapping,
          NO inbound permission — orphan Lambdas that exist for no reason.

Methodology:
  1. Enumerate all Lambdas + their resolved triggers (EB + Scheduler v2 + URL + ESM)
  2. Compute per-Lambda 'expected invocations per 7d' from cron/rate expressions
  3. Pull actual CW invocations 7d
  4. Pull actual S3 freshness per key
  5. Cross-correlate to surface silent failures
  6. Rank by severity (production-critical first)

Writes: aws/ops/reports/1032.json
"""
import json, boto3, os, re
from datetime import datetime, timezone, timedelta
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed

REGION = 'us-east-1'
BUCKET = 'justhodl-dashboard-live'
NOW = datetime.now(timezone.utc)

lam = boto3.client('lambda', region_name=REGION)
events = boto3.client('events', region_name=REGION)
sched = boto3.client('scheduler', region_name=REGION)
cw = boto3.client('cloudwatch', region_name=REGION)
s3 = boto3.client('s3', region_name=REGION)
logs = boto3.client('logs', region_name=REGION)

report = {'started_at': NOW.isoformat()}


# ---- Helpers ----

def expected_invocations_per_7d(expr):
    """Convert cron/rate expression to expected invocations per 7 days."""
    if not expr:
        return None
    expr = expr.strip()
    # rate(N minutes/hours/days)
    m = re.match(r'rate\((\d+)\s*(minute|minutes|hour|hours|day|days)\)', expr)
    if m:
        n = int(m.group(1)); unit = m.group(2)
        if 'minute' in unit:
            return (7 * 24 * 60) // n
        if 'hour' in unit:
            return (7 * 24) // n
        if 'day' in unit:
            return 7 // n
    # cron(min hour day-of-month month day-of-week year)
    cm = re.match(r'cron\(([^)]+)\)', expr)
    if cm:
        parts = cm.group(1).split()
        if len(parts) >= 5:
            minute_part, hour_part = parts[0], parts[1]
            dom, month, dow = parts[2], parts[3], parts[4]
            # Estimate fires per day
            def count_values(p, max_val):
                if p in ('*', '?'): return max_val
                if p.startswith('*/'):
                    return max_val // int(p[2:])
                if ',' in p:
                    return len(p.split(','))
                if '-' in p:
                    a,b = p.split('-')
                    return int(b) - int(a) + 1
                return 1
            fires_per_hour = count_values(minute_part, 60)
            hours_per_day = count_values(hour_part, 24)
            # day-of-week constraint
            if dow not in ('*', '?'):
                # e.g. MON-FRI = 5/7 of days, FRI = 1/7
                if dow == 'MON-FRI': dow_frac = 5/7
                elif dow == 'SAT,SUN': dow_frac = 2/7
                elif ',' in dow: dow_frac = len(dow.split(','))/7
                elif '-' in dow:
                    days_map = {'SUN':0,'MON':1,'TUE':2,'WED':3,'THU':4,'FRI':5,'SAT':6}
                    a,b = dow.split('-')
                    try: dow_frac = (days_map.get(b,0)-days_map.get(a,0)+1)/7
                    except: dow_frac = 1/7
                else: dow_frac = 1/7
            else:
                dow_frac = 1.0
            # day-of-month constraint
            if dom not in ('*','?','*/1'):
                dom_frac = 1/30  # specific day of month
            else:
                dom_frac = 1.0
            return int(fires_per_hour * hours_per_day * 7 * dow_frac * dom_frac)
    return None


def expected_max_output_age_hours(expr):
    """Convert schedule expression to expected max output age in hours
    (2× schedule cadence — slack for one missed run)."""
    if not expr: return None
    m = re.match(r'rate\((\d+)\s*(minute|minutes|hour|hours|day|days)\)', expr)
    if m:
        n = int(m.group(1)); unit = m.group(2)
        if 'minute' in unit: return (n / 60) * 2
        if 'hour' in unit:   return n * 2
        if 'day' in unit:    return n * 24 * 2
    cm = re.match(r'cron\(([^)]+)\)', expr)
    if cm:
        parts = cm.group(1).split()
        # Approximate cron cadence: if FRI-only -> 168 (1 week)
        if len(parts) >= 5:
            dow = parts[4]
            dom = parts[2]
            hour = parts[1]
            minute = parts[0]
            if dow not in ('*','?'):
                if dow == 'SUN' or dow == 'FRI' or dow == 'MON': return 168 * 2  # weekly
                if dow == 'MON-FRI': return 24 * 2 * (7/5)  # business daily
            if dom not in ('*','?','*/1'):
                return 30 * 24 * 2  # monthly
            # If hour is specific (e.g. 0 12 * * ? *) → daily
            if minute.isdigit() and hour.isdigit():
                return 24 * 2  # daily
            if minute.isdigit() and hour == '*':
                return 1 * 2  # hourly
    return None


# ---- Enumerate Lambdas + their schedules ----
print("[1] Enumerating Lambdas and schedules...")
all_lambdas = []
paginator = lam.get_paginator('list_functions')
for page in paginator.paginate():
    all_lambdas.extend(page['Functions'])
print(f"  {len(all_lambdas)} Lambdas")

# All EB rules + targets
print("[2] EB rules + targets...")
eb_rules = []
paginator = events.get_paginator('list_rules')
for page in paginator.paginate():
    eb_rules.extend(page.get('Rules', []))

eb_targets = defaultdict(list)  # lambda_name -> [rule details]
for r in eb_rules:
    try:
        ts = events.list_targets_by_rule(Rule=r['Name']).get('Targets', [])
        for t in ts:
            arn = t.get('Arn', '')
            if ':function:' in arn:
                fn_name = arn.split(':function:')[-1].split(':')[0]
                eb_targets[fn_name].append({
                    'rule': r['Name'],
                    'state': r.get('State'),
                    'expression': r.get('ScheduleExpression'),
                })
    except Exception: pass

# All Scheduler v2
print("[3] Scheduler v2 schedules...")
sv2_schedules = []
paginator = sched.get_paginator('list_schedules')
for page in paginator.paginate():
    sv2_schedules.extend(page.get('Schedules', []))

sv2_targets = defaultdict(list)
for s in sv2_schedules:
    arn = (s.get('Target') or {}).get('Arn', '')
    if ':function:' in arn:
        fn_name = arn.split(':function:')[-1].split(':')[0]
        # Get the expression
        try:
            d = sched.get_schedule(Name=s['Name'], GroupName=s.get('GroupName', 'default'))
            sv2_targets[fn_name].append({
                'schedule': s['Name'],
                'state': d.get('State'),
                'expression': d.get('ScheduleExpression'),
            })
        except Exception: pass

# ---- CW invocations + errors 7d for all Lambdas ----
print("[4] CloudWatch 7d metrics...")
end = NOW
start = end - timedelta(days=7)

def get_metrics(name):
    out = {}
    for metric in ['Invocations', 'Errors', 'Throttles']:
        try:
            resp = cw.get_metric_statistics(
                Namespace='AWS/Lambda', MetricName=metric,
                Dimensions=[{'Name': 'FunctionName', 'Value': name}],
                StartTime=start, EndTime=end, Period=86400, Statistics=['Sum'],
            )
            out[metric.lower()] = int(sum(p['Sum'] for p in resp['Datapoints']))
            # Last non-zero datapoint
            if metric == 'Invocations':
                dps = sorted(resp['Datapoints'], key=lambda p: p['Timestamp'], reverse=True)
                for p in dps:
                    if p['Sum'] > 0:
                        out['last_invocation_day'] = p['Timestamp'].date().isoformat()
                        break
        except Exception:
            out[metric.lower()] = None
    return name, out

cw_metrics = {}
with ThreadPoolExecutor(max_workers=16) as ex:
    futures = [ex.submit(get_metrics, fn['FunctionName']) for fn in all_lambdas]
    for fut in as_completed(futures):
        name, m = fut.result()
        cw_metrics[name] = m

# ---- S3 inventory (all data/* keys + LastModified) ----
print("[5] S3 inventory...")
s3_keys = {}  # key -> {age_h, size, last_modified}
paginator = s3.get_paginator('list_objects_v2')
for page in paginator.paginate(Bucket=BUCKET, Prefix='data/'):
    for obj in page.get('Contents', []):
        s3_keys[obj['Key']] = {
            'age_h': round((NOW - obj['LastModified']).total_seconds() / 3600, 1),
            'size': obj['Size'],
            'last_modified': obj['LastModified'].isoformat(),
        }
print(f"  {len(s3_keys)} keys")

# ---- TYPE A: schedule ENABLED but invocations < 50% of expected ----
print("[6] TYPE A: invocation chain breaks...")
type_a = []
for fn in all_lambdas:
    name = fn['FunctionName']
    triggers = []
    for r in eb_targets.get(name, []):
        if r['state'] == 'ENABLED' and r.get('expression'):
            exp = expected_invocations_per_7d(r['expression'])
            triggers.append({'kind': 'EB', 'rule': r['rule'], 'expr': r['expression'], 'expected_7d': exp})
    for s in sv2_targets.get(name, []):
        if s['state'] == 'ENABLED' and s.get('expression'):
            exp = expected_invocations_per_7d(s['expression'])
            triggers.append({'kind': 'SV2', 'schedule': s['schedule'], 'expr': s['expression'], 'expected_7d': exp})
    if not triggers: continue
    total_expected = sum((t['expected_7d'] or 0) for t in triggers)
    actual = cw_metrics.get(name, {}).get('invocations', 0) or 0
    if total_expected == 0: continue
    # Allow 50% tolerance — flag if actual < 50% of expected
    if actual < (total_expected * 0.5):
        severity = 'CRITICAL' if actual == 0 else 'HIGH'
        type_a.append({
            'lambda': name,
            'severity': severity,
            'expected_7d': total_expected,
            'actual_7d': actual,
            'ratio': round(actual/total_expected, 2) if total_expected else 0,
            'triggers': triggers,
            'last_invocation_day': cw_metrics.get(name, {}).get('last_invocation_day'),
        })

# Skip Lambdas that just got their schedule today (created < 36h ago) - false positive guard
all_schedule_creation_dates = {}
for s in sv2_schedules:
    try:
        d = sched.get_schedule(Name=s['Name'], GroupName=s.get('GroupName', 'default'))
        creation = d.get('CreationDate')
        if creation:
            arn = (d.get('Target') or {}).get('Arn', '')
            if ':function:' in arn:
                fn_name = arn.split(':function:')[-1].split(':')[0]
                all_schedule_creation_dates[fn_name] = creation
    except Exception: pass

# Filter out brand-new schedules
filtered_type_a = []
for entry in type_a:
    name = entry['lambda']
    creation = all_schedule_creation_dates.get(name)
    if creation:
        age_h = (NOW - creation).total_seconds() / 3600
        if age_h < 36:
            entry['schedule_age_hours'] = round(age_h, 1)
            entry['note'] = 'schedule just created — too early to flag'
            continue  # skip
    filtered_type_a.append(entry)

type_a = sorted(filtered_type_a, key=lambda x: (x['severity']!='CRITICAL', -x['expected_7d']))
report['TYPE_A_invocation_chain_broken'] = type_a

# ---- TYPE B: invokes successfully but output stale ----
print("[7] TYPE B: silent S3 write failures...")
# For each S3 key, find probable producer using cross-graph (from 1021).
# Use the heuristic: a Lambda is a "real producer" if its name shows up in BOTH
# a put-context AND the key contains its likely output name.
# Simpler: load 1021 cross-graph
try:
    with open('aws/ops/reports/1021.json') as f:
        cg_data = json.load(f)
    cg = cg_data.get('cross_graph', {})
except Exception:
    cg = {}

# For each Lambda, find its likely output keys (where it's listed as producer
# AND the key contains its short-name)
type_b = []
for fn in all_lambdas:
    name = fn['FunctionName']
    if not name.startswith('justhodl-'): continue
    short = name.replace('justhodl-', '')
    inv7d = cw_metrics.get(name, {}).get('invocations', 0) or 0
    if inv7d == 0: continue
    # Find triggers + max expected output age
    expected_max_age = None
    for r in eb_targets.get(name, []):
        if r['state'] == 'ENABLED':
            a = expected_max_output_age_hours(r.get('expression'))
            if a: expected_max_age = min(expected_max_age, a) if expected_max_age else a
    for s in sv2_targets.get(name, []):
        if s['state'] == 'ENABLED':
            a = expected_max_output_age_hours(s.get('expression'))
            if a: expected_max_age = min(expected_max_age, a) if expected_max_age else a
    if not expected_max_age: continue
    # Find candidate output keys (Lambda is producer + key name matches short name OR vice versa)
    candidates = []
    for key, info in cg.items():
        producers = info.get('producers', []) if isinstance(info, dict) else []
        if name not in producers: continue
        # Name match heuristic: short name in key OR key root in name
        key_root = key.replace('data/','').split('.')[0].split('/')[0]
        if short in key or key_root in short or key_root.replace('-','_') in short.replace('-','_'):
            actual = s3_keys.get(key)
            if actual:
                candidates.append({
                    'key': key,
                    'age_h': actual['age_h'],
                    'expected_max_age_h': round(expected_max_age, 1),
                    'stale': actual['age_h'] > expected_max_age,
                    'other_producers': [p for p in producers if p != name],
                })
    # Flag if ANY candidate is stale
    stale = [c for c in candidates if c['stale']]
    if stale:
        type_b.append({
            'lambda': name,
            'invocations_7d': inv7d,
            'expected_max_age_h': round(expected_max_age, 1),
            'stale_outputs': stale[:5],
            'severity': 'CRITICAL' if any(c['age_h']/c['expected_max_age_h'] > 10 for c in stale) else 'HIGH',
        })

type_b = sorted(type_b, key=lambda x: -max(c['age_h']/c['expected_max_age_h'] for c in x['stale_outputs']))
report['TYPE_B_silent_s3_write_failures'] = type_b[:50]

# ---- TYPE C: errors with no alarm ----
print("[8] TYPE C: silent errors (errors > 0, no alarm)...")
# Get all alarms
alarms = []
paginator = cw.get_paginator('describe_alarms')
for page in paginator.paginate():
    for a in page.get('MetricAlarms', []):
        for d in a.get('Dimensions', []):
            if d['Name'] == 'FunctionName':
                alarms.append({'lambda': d['Value'], 'metric': a.get('MetricName')})
alarmed_lambdas = {a['lambda'] for a in alarms}

type_c = []
for name, m in cw_metrics.items():
    errs = m.get('errors', 0) or 0
    if errs > 0 and name not in alarmed_lambdas:
        inv = m.get('invocations', 0) or 1
        type_c.append({
            'lambda': name,
            'errors_7d': errs,
            'invocations_7d': inv,
            'error_rate': round(errs/inv*100, 1),
        })
type_c = sorted(type_c, key=lambda x: -x['errors_7d'])
report['TYPE_C_silent_errors'] = type_c

# ---- TYPE D: asymmetric writers ----
print("[9] TYPE D: race conditions where only one writer is alive...")
type_d = []
# We have race-condition keys: data/report.json, calibration/latest.json
# Check each: who actually wrote it most recently
key_to_writers = {
    'data/report.json': ['justhodl-bloomberg-v8', 'justhodl-daily-report-v3'],
    'calibration/latest.json': ['justhodl-calibration-snapshotter', 'justhodl-calibrator'],
}
for key, writers in key_to_writers.items():
    fresh = s3_keys.get(key)
    if not fresh: continue
    file_mod = datetime.fromisoformat(fresh['last_modified'].replace('Z','+00:00'))
    # For each writer, find last invocation time AND check error rate
    writer_info = []
    for w in writers:
        wm = cw_metrics.get(w, {})
        last_inv_day = wm.get('last_invocation_day')
        writer_info.append({
            'lambda': w,
            'invocations_7d': wm.get('invocations', 0),
            'errors_7d': wm.get('errors', 0),
            'last_invocation_day': last_inv_day,
        })
    type_d.append({
        'key': key,
        'file_last_modified': fresh['last_modified'],
        'file_age_h': fresh['age_h'],
        'writers': writer_info,
    })
report['TYPE_D_asymmetric_race_writers'] = type_d

# ---- TYPE E: truly orphan Lambdas ----
print("[10] TYPE E: orphan Lambdas with no trigger...")
type_e = []
for fn in all_lambdas:
    name = fn['FunctionName']
    has_eb = bool(eb_targets.get(name))
    has_sv2 = bool(sv2_targets.get(name))
    # Check function URL + ESM + resource policy
    has_url = False; has_esm = False; has_perm = False
    try:
        lam.get_function_url_config(FunctionName=name); has_url = True
    except lam.exceptions.ResourceNotFoundException: pass
    except Exception: pass
    try:
        esm = lam.list_event_source_mappings(FunctionName=name)
        has_esm = len(esm.get('EventSourceMappings', [])) > 0
    except Exception: pass
    try:
        pol = lam.get_policy(FunctionName=name)
        has_perm = True
    except lam.exceptions.ResourceNotFoundException: pass
    except Exception: pass
    
    inv7d = cw_metrics.get(name, {}).get('invocations', 0) or 0
    if not has_eb and not has_sv2 and not has_url and not has_esm and not has_perm and inv7d == 0:
        type_e.append({
            'lambda': name,
            'invocations_7d': 0,
            'last_modified': fn.get('LastModified'),
        })
report['TYPE_E_truly_orphan'] = sorted(type_e, key=lambda x: x['lambda'])

# Summary
report['summary'] = {
    'n_lambdas': len(all_lambdas),
    'TYPE_A_invocation_breaks_critical': len([x for x in type_a if x['severity']=='CRITICAL']),
    'TYPE_A_invocation_breaks_high': len([x for x in type_a if x['severity']=='HIGH']),
    'TYPE_B_silent_s3_write_critical': len([x for x in type_b if x['severity']=='CRITICAL']),
    'TYPE_B_silent_s3_write_high': len([x for x in type_b if x['severity']=='HIGH']),
    'TYPE_C_silent_errors': len(type_c),
    'TYPE_D_race_winners': len(type_d),
    'TYPE_E_orphan_lambdas': len(type_e),
}

# Write report
os.makedirs('aws/ops/reports', exist_ok=True)
with open('aws/ops/reports/1032.json', 'w') as f:
    json.dump(report, f, indent=2, default=str)

print("\n=== SUMMARY ===")
print(json.dumps(report['summary'], indent=2))
print(f"\nFull report: aws/ops/reports/1032.json")
