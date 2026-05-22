#!/usr/bin/env python3
"""
ops 1030 — VERIFY audit findings before Khalid acts on them.

Targeted re-verification of high-stakes claims from ops 1029:
  1. R-series Lambdas with "no log group" — is that really true?
     Could be: (a) really never invoked, (b) log group exists but log streams empty,
              (c) recently created with no retention, (d) URL/API-triggered with logs elsewhere.
  2. ZOMBIE lambdas — verify last invocation timestamp via CloudWatch metrics (not just logs)
  3. DLQ=0 claim — re-check, and also check async OnFailure destinations (alt to DLQ)
  4. portfolio-catalysts 76.7% error — sanity check via metrics window and recent log sample
  5. forced-selling-bounce 40% error — same

Writes: aws/ops/reports/1030.json
"""
import json, boto3, os
from datetime import datetime, timezone, timedelta

REGION = 'us-east-1'
NOW = datetime.now(timezone.utc)
lam = boto3.client('lambda', region_name=REGION)
logs = boto3.client('logs', region_name=REGION)
cw  = boto3.client('cloudwatch', region_name=REGION)
events = boto3.client('events', region_name=REGION)
sched = boto3.client('scheduler', region_name=REGION)

R_SERIES = [
    'justhodl-sec-filing-diff', 'justhodl-transcript-indexer', 'justhodl-transcript-query',
    'justhodl-peer-comparison', 'justhodl-screen-builder', 'justhodl-fedwatch-rate-probability',
    'justhodl-supply-chain-linkage', 'justhodl-cftc-deep-view',
    'justhodl-fx-decomposition', 'justhodl-factor-decomposition',
]

OTHER_FLAGGED = [
    'macro-report-api', 'autonomous-ai-processor', 'justhodl-email-reports',
    'justhodl-daily-macro-report',
]

HIGH_ERROR = [
    'justhodl-portfolio-catalysts',
    'justhodl-forced-selling-bounce',
    'justhodl-watchlist-debate',
    'justhodl-email-reports-v2',
    'justhodl-nobrainer-rationale',
]

report = {'started_at': NOW.isoformat(), 'r_series': {}, 'other_flagged': {}, 'high_error': {}, 'dlq_recheck': {}}


def deep_check(name):
    out = {'name': name}

    # 1. Does Lambda exist?
    try:
        cfg = lam.get_function_configuration(FunctionName=name)
        out['exists'] = True
        out['last_modified'] = cfg.get('LastModified')
        out['runtime'] = cfg.get('Runtime')
        out['code_sha'] = cfg.get('CodeSha256', '')[:12]
        # DLQ check
        dlq = (cfg.get('DeadLetterConfig') or {}).get('TargetArn')
        out['dlq_arn'] = dlq if dlq else None
    except lam.exceptions.ResourceNotFoundException:
        out['exists'] = False
        return out
    except Exception as e:
        out['error'] = str(e)
        return out

    # 2. Async destination (alt to DLQ)
    try:
        ec = lam.get_function_event_invoke_config(FunctionName=name)
        out['async_on_failure'] = (ec.get('DestinationConfig') or {}).get('OnFailure', {}).get('Destination')
    except lam.exceptions.ResourceNotFoundException:
        out['async_on_failure'] = None
    except Exception:
        out['async_on_failure'] = 'unknown'

    # 3. Function URL?
    try:
        u = lam.get_function_url_config(FunctionName=name)
        out['function_url'] = u.get('FunctionUrl')
    except lam.exceptions.ResourceNotFoundException:
        out['function_url'] = None
    except Exception:
        out['function_url'] = 'unknown'

    # 4. Event source mappings (SQS/Kinesis/DDB triggers)
    try:
        esm = lam.list_event_source_mappings(FunctionName=name)
        out['event_source_mappings'] = [m.get('EventSourceArn', '').split(':')[-1] for m in esm.get('EventSourceMappings', [])]
    except Exception:
        out['event_source_mappings'] = []

    # 5. Lambda permissions (API Gateway, EventBridge invokes)
    try:
        pol_resp = lam.get_policy(FunctionName=name)
        pol = json.loads(pol_resp['Policy'])
        invokers = set()
        for st in pol.get('Statement', []):
            principal = st.get('Principal', {})
            svc = principal.get('Service', '')
            if svc: invokers.add(svc)
            src = st.get('Condition', {}).get('ArnLike', {}).get('AWS:SourceArn', '')
            if src: invokers.add(src.split(':')[2] if ':' in src else src)
        out['invokers_from_policy'] = sorted(invokers)
    except lam.exceptions.ResourceNotFoundException:
        out['invokers_from_policy'] = []
    except Exception:
        out['invokers_from_policy'] = 'unknown'

    # 6. EventBridge rules targeting this Lambda
    try:
        rules_targeting = []
        paginator = events.get_paginator('list_rule_names_by_target')
        for page in paginator.paginate(TargetArn=cfg['FunctionArn']):
            for rn in page.get('RuleNames', []):
                try:
                    rd = events.describe_rule(Name=rn)
                    rules_targeting.append({'name': rn, 'schedule': rd.get('ScheduleExpression', ''), 'state': rd.get('State')})
                except Exception: pass
        out['eventbridge_rules_targeting'] = rules_targeting
    except Exception as e:
        out['eventbridge_rules_targeting'] = f'error: {str(e)[:80]}'

    # 7. Scheduler v2 schedules with this target
    out['scheduler_v2_schedules'] = []
    try:
        paginator = sched.get_paginator('list_schedules')
        for page in paginator.paginate():
            for s in page.get('Schedules', []):
                tgt = (s.get('Target') or {}).get('Arn', '')
                if name in tgt or tgt.endswith(':function:' + name):
                    # need full detail to know expression
                    try:
                        d = sched.get_schedule(Name=s['Name'], GroupName=s.get('GroupName', 'default'))
                        out['scheduler_v2_schedules'].append({
                            'name': s['Name'],
                            'state': d.get('State'),
                            'expression': d.get('ScheduleExpression'),
                        })
                    except Exception: pass
    except Exception as e:
        out['scheduler_v2_schedules'] = f'error: {str(e)[:80]}'

    # 8. Log group: does it EXIST? (separate from log streams)
    lg = f'/aws/lambda/{name}'
    try:
        resp = logs.describe_log_groups(logGroupNamePrefix=lg, limit=1)
        groups = resp.get('logGroups', [])
        match = [g for g in groups if g.get('logGroupName') == lg]
        if match:
            g = match[0]
            out['log_group_exists'] = True
            out['log_group_created'] = datetime.fromtimestamp(g['creationTime']/1000, timezone.utc).isoformat()
            out['log_group_stored_bytes'] = g.get('storedBytes', 0)
        else:
            out['log_group_exists'] = False
    except Exception as e:
        out['log_group_exists'] = f'error: {str(e)[:80]}'

    # 9. Latest log stream
    try:
        ls = logs.describe_log_streams(logGroupName=lg, orderBy='LastEventTime', descending=True, limit=1)
        streams = ls.get('logStreams', [])
        if streams:
            s = streams[0]
            last_event = s.get('lastEventTimestamp')
            out['latest_log_stream'] = {
                'name': s['logStreamName'],
                'last_event': datetime.fromtimestamp(last_event/1000, timezone.utc).isoformat() if last_event else None,
                'days_ago': round((NOW.timestamp()*1000 - last_event) / 86400000, 1) if last_event else None,
            }
        else:
            out['latest_log_stream'] = None
    except logs.exceptions.ResourceNotFoundException:
        out['latest_log_stream'] = 'no_log_group'
    except Exception as e:
        out['latest_log_stream'] = f'error: {str(e)[:80]}'

    # 10. CloudWatch Invocations metric over LAST 30 DAYS (more thorough than 7d)
    try:
        end = NOW
        start = end - timedelta(days=30)
        resp = cw.get_metric_statistics(
            Namespace='AWS/Lambda', MetricName='Invocations',
            Dimensions=[{'Name': 'FunctionName', 'Value': name}],
            StartTime=start, EndTime=end, Period=86400, Statistics=['Sum'],
        )
        invs_30d = int(sum(p['Sum'] for p in resp['Datapoints']))
        out['invocations_30d'] = invs_30d
        # Also get last invocation date by finding latest non-zero datapoint
        dps = sorted(resp['Datapoints'], key=lambda p: p['Timestamp'])
        last_invoked = None
        for p in reversed(dps):
            if p['Sum'] > 0:
                last_invoked = p['Timestamp']
                break
        out['last_invocation_date'] = last_invoked.isoformat() if last_invoked else None
    except Exception as e:
        out['invocations_30d'] = f'error: {str(e)[:80]}'

    return out


# Run R-series deep check
print("="*60)
print("R-SERIES DEEP VERIFICATION")
print("="*60)
for name in R_SERIES + OTHER_FLAGGED:
    print(f"  {name}...")
    report['r_series' if name.startswith('justhodl') and name in R_SERIES else 'other_flagged'][name] = deep_check(name)

# High-error deep check
print("\n=== HIGH-ERROR DEEP CHECK ===")
for name in HIGH_ERROR:
    print(f"  {name}...")
    d = deep_check(name)
    # Also pull a sample of recent error log lines to see WHAT is failing
    lg = f'/aws/lambda/{name}'
    try:
        end_ms = int(NOW.timestamp()*1000)
        start_ms = end_ms - 7*86400*1000
        q = logs.filter_log_events(
            logGroupName=lg, startTime=start_ms, endTime=end_ms,
            filterPattern='?ERROR ?Error ?Exception ?Traceback', limit=10,
        )
        d['recent_error_lines'] = [
            {'ts': datetime.fromtimestamp(e['timestamp']/1000, timezone.utc).isoformat(),
             'msg': e['message'][:300]}
            for e in q.get('events', [])[:5]
        ]
    except Exception as e:
        d['recent_error_lines'] = f'error: {str(e)[:100]}'
    report['high_error'][name] = d

# DLQ recheck: list ALL lambdas with ANY DLQ or async OnFailure destination
print("\n=== DLQ + async destination RECHECK across all Lambdas ===")
all_lambdas = []
paginator = lam.get_paginator('list_functions')
for page in paginator.paginate():
    all_lambdas.extend(page['Functions'])

dlq_count = 0
async_dest_count = 0
for fn in all_lambdas:
    cfg = fn  # list_functions has the config
    if (cfg.get('DeadLetterConfig') or {}).get('TargetArn'):
        dlq_count += 1
        report['dlq_recheck'].setdefault('with_dlq', []).append(cfg['FunctionName'])
    # async dest needs separate call — too slow for 399; sample 30
report['dlq_recheck']['n_lambdas_total'] = len(all_lambdas)
report['dlq_recheck']['n_with_dlq'] = dlq_count

# Sample 30 random Lambdas for async destination check
import random
sample = random.sample(all_lambdas, min(30, len(all_lambdas)))
async_sampled = []
for fn in sample:
    try:
        ec = lam.get_function_event_invoke_config(FunctionName=fn['FunctionName'])
        dest = (ec.get('DestinationConfig') or {}).get('OnFailure', {}).get('Destination')
        if dest:
            async_sampled.append({'lambda': fn['FunctionName'], 'on_failure': dest})
    except lam.exceptions.ResourceNotFoundException:
        pass
    except Exception:
        pass
report['dlq_recheck']['async_dest_sample_30'] = async_sampled

# Write report
report['completed_at'] = datetime.now(timezone.utc).isoformat()
os.makedirs('aws/ops/reports', exist_ok=True)
with open('aws/ops/reports/1030.json', 'w') as f:
    json.dump(report, f, indent=2, default=str)

# Concise console summary
print("\n" + "="*70)
print("VERIFICATION SUMMARY")
print("="*70)
print("\nR-SERIES — truly never invoked?")
for n in R_SERIES:
    d = report.get('r_series', {}).get(n, {}) or report.get('other_flagged', {}).get(n, {})
    if not d: continue
    inv = d.get('invocations_30d')
    lge = d.get('log_group_exists')
    eb_rules = len(d.get('eventbridge_rules_targeting', [])) if isinstance(d.get('eventbridge_rules_targeting'), list) else '?'
    sv2 = len(d.get('scheduler_v2_schedules', [])) if isinstance(d.get('scheduler_v2_schedules'), list) else '?'
    url = bool(d.get('function_url'))
    invokers = d.get('invokers_from_policy', [])
    verdict = "CONFIRMED dead" if (inv == 0 and not eb_rules and not sv2 and not url and not invokers) else "HAS TRIGGER"
    print(f"  {n}")
    print(f"    inv_30d={inv}  log_grp={lge}  eb_rules={eb_rules}  sched_v2={sv2}  url={url}  invokers={invokers}")
    print(f"    → {verdict}")

print(f"\nDLQ recheck: {report['dlq_recheck']['n_with_dlq']}/{report['dlq_recheck']['n_lambdas_total']} have DLQ")
print(f"Async OnFailure (sample of 30): {len(report['dlq_recheck']['async_dest_sample_30'])} have it")

print(f"\nFull report: aws/ops/reports/1030.json")
