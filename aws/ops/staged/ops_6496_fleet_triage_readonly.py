"""Read-only fleet triage for the 2026-10-08 audit (Perplexity lane).

Collects, without any AWS write or invocation:
  A. live schedule bindings (classic rules + Scheduler) per function
  B. 30-day Invocations/Errors/Throttles per function (CloudWatch GetMetricData)
  C. every contract violation -> producer functions -> schedule/health class
  D. principal-row history max per artifact (data/_state/rowcounts/*.json)
  E. functions on retired runtimes (python3.9 / python3.10 / nodejs18.x)
  F. capital-structure refresh control state (why the daily plan step fails)
  G. ops queue: scripts the serial lane would run on the next pending push

Writes one markdown report with an embedded JSON block. Nothing else.
"""
from datetime import datetime, timedelta, timezone
from pathlib import Path
import collections
import json
import re
import subprocess
import sys

import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[3]
sys.path[:0] = [str(ROOT / 'aws/lambdas/justhodl-contract-gate/source'), str(ROOT / 'aws/ops/checks'),
                str(ROOT / 'aws/shared')]
from reviewed_contracts import apply_producers, artifact_function_name  # noqa: E402

REPORT = ROOT / 'aws/ops/reports/latest/ops_6496_fleet_triage_readonly.md'
BUCKET = 'justhodl-dashboard-live'
CFG = Config(retries={'max_attempts': 10, 'mode': 'adaptive'}, read_timeout=60)
s3 = boto3.client('s3', config=CFG)
lam = boto3.client('lambda', config=CFG)
cw = boto3.client('cloudwatch', config=CFG)
ev = boto3.client('events', config=CFG)
sch = boto3.client('scheduler', config=CFG)
NOW = datetime.now(timezone.utc)
RETIRED = {'python3.9', 'python3.10', 'nodejs18.x'}
out = {'ran_at': NOW.isoformat(), 'notes': []}


def get_json(key):
    return json.loads(s3.get_object(Bucket=BUCKET, Key=key)['Body'].read())


def log(msg):
    print('[triage] %s' % msg, flush=True)


# ---------------------------------------------------------------- A. functions + schedules
functions = {}
for page in lam.get_paginator('list_functions').paginate():
    for f in page['Functions']:
        functions[f['FunctionName']] = {
            'runtime': f.get('Runtime', 'image'), 'handler': f.get('Handler'),
            'last_modified': f.get('LastModified'), 'memory': f.get('MemorySize'),
            'timeout': f.get('Timeout'), 'description': (f.get('Description') or '')[:120],
            'layers': [l['Arn'].split(':')[-2] + ':' + l['Arn'].split(':')[-1] for l in f.get('Layers', [])],
            'package_type': f.get('PackageType', 'Zip'), 'code_size': f.get('CodeSize'),
            'in_repo': (ROOT / 'aws/lambdas' / f['FunctionName']).is_dir(),
            'schedules': [],
        }
log('%d functions' % len(functions))

rule_count = 0
for page in ev.get_paginator('list_rules').paginate():
    for r in page['Rules']:
        rule_count += 1
        try:
            targets = ev.list_targets_by_rule(Rule=r['Name']).get('Targets', [])
        except Exception as e:
            out['notes'].append('list_targets_by_rule %s: %s' % (r['Name'], str(e)[:80]))
            continue
        for t in targets:
            fn = artifact_function_name(t.get('Arn'))
            if fn in functions:
                functions[fn]['schedules'].append({'kind': 'events', 'name': r['Name'],
                                                   'state': r.get('State'), 'expr': r.get('ScheduleExpression')})
sched_count = 0
for page in sch.get_paginator('list_schedules').paginate():
    for s in page['Schedules']:
        sched_count += 1
        fn = artifact_function_name((s.get('Target') or {}).get('Arn'))
        if fn in functions:
            functions[fn]['schedules'].append({'kind': 'scheduler', 'name': s['Name'], 'group': s.get('GroupName'),
                                               'state': s.get('State'), 'expr': None})
log('%d classic rules, %d schedules' % (rule_count, sched_count))
out['schedule_totals'] = {'classic_rules': rule_count, 'scheduler_schedules': sched_count}

# ---------------------------------------------------------------- B. 30d metrics
start = NOW - timedelta(days=30)
names = sorted(functions)
metrics = {n: {'inv30': 0, 'err30': 0, 'thr30': 0, 'inv7': 0, 'err7': 0, 'last_inv_day': None} for n in names}
queries = []
for i, n in enumerate(names):
    for stat in ('Invocations', 'Errors', 'Throttles'):
        queries.append({'Id': 'q%d_%s' % (i, stat[0]), 'ReturnData': True,
                        'MetricStat': {'Metric': {'Namespace': 'AWS/Lambda', 'MetricName': stat,
                                                  'Dimensions': [{'Name': 'FunctionName', 'Value': n}]},
                                       'Period': 86400, 'Stat': 'Sum'}})
for i in range(0, len(queries), 500):
    batch = queries[i:i + 500]
    token = None
    while True:
        kw = {'MetricDataQueries': batch, 'StartTime': start, 'EndTime': NOW, 'ScanBy': 'TimestampDescending'}
        if token:
            kw['NextToken'] = token
        resp = cw.get_metric_data(**kw)
        for r in resp['MetricDataResults']:
            idx, stat = r['Id'][1:].split('_')
            n = names[int(idx)]
            key = {'I': 'inv', 'E': 'err', 'T': 'thr'}[stat]
            for ts, val in zip(r['Timestamps'], r['Values']):
                metrics[n][key + '30'] += val
                if ts >= NOW - timedelta(days=7) and key != 'thr':
                    metrics[n][key + '7'] += val
                if key == 'inv' and val > 0:
                    d = ts.date().isoformat()
                    if metrics[n]['last_inv_day'] is None or d > metrics[n]['last_inv_day']:
                        metrics[n]['last_inv_day'] = d
        token = resp.get('NextToken')
        if not token:
            break
for n in names:
    functions[n].update(metrics[n])
log('metrics collected')


def health(fn):
    f = functions.get(fn)
    if not f:
        return 'FUNCTION_MISSING'
    enabled = any(s['state'] == 'ENABLED' for s in f['schedules'])
    if f['inv30'] == 0:
        return 'NO_INVOCATIONS_30D' + ('_SCHEDULED' if enabled else '_UNSCHEDULED')
    if f['inv7'] and f['err7'] >= 0.9 * f['inv7']:
        return 'ALL_ERRORS_7D'
    if f['err7'] > 0:
        return 'SOME_ERRORS_7D'
    if f['inv7'] == 0:
        return 'IDLE_7D'
    return 'RUNNING_CLEAN'


# ---------------------------------------------------------------- D. rowcount history
rowmax = collections.defaultdict(int)
rowdays = 0
for page in s3.get_paginator('list_objects_v2').paginate(Bucket=BUCKET, Prefix='data/_state/rowcounts/'):
    for o in page.get('Contents', []):
        try:
            rows = get_json(o['Key']).get('rows') or {}
        except Exception:
            continue
        rowdays += 1
        for k, v in rows.items():
            if isinstance(v, (int, float)) and v > rowmax[k]:
                rowmax[k] = int(v)
out['rowcount_history_days'] = rowdays
log('rowcount history: %d days, %d artifacts' % (rowdays, len(rowmax)))

# ---------------------------------------------------------------- C. violations -> producers
cv = get_json('data/contract-violations.json')
prod = apply_producers(get_json('config/artifact-producers.json'))['producers']
contracts = get_json('config/engine-contracts.json').get('contracts', {})
triage = []
for v in cv['violations']:
    writers = (prod.get(v['artifact']) or {}).get('writers') or []
    hs = {w: health(w) for w in writers}
    c = contracts.get(v['artifact']) or {}
    triage.append({'cls': v['cls'], 'artifact': v['artifact'], 'detail': v['detail'][:140], 'writers': hs,
                   'contract_floor': c.get('min_rows'), 'learned_rows': c.get('learned_rows'),
                   'history_max_rows': rowmax.get(v['artifact'])})
out['violations'] = triage
summary = collections.Counter()
for t in triage:
    if not t['writers']:
        cls = 'NO_WRITER'
    else:
        hs = set(t['writers'].values())
        cls = 'RUNNING_CLEAN' if hs == {'RUNNING_CLEAN'} else sorted(h for h in hs if h != 'RUNNING_CLEAN')[0]
    summary[(t['cls'], cls)] += 1
out['violation_summary'] = {'%s|%s' % k: n for k, n in sorted(summary.items())}
unc = cv.get('uncontracted') or []
out['uncontracted'] = {k: {w: health(w) for w in ((prod.get(k) or {}).get('writers') or [])} for k in unc}

# ---------------------------------------------------------------- E. retired runtimes
out['retired_runtime_functions'] = {n: f for n, f in functions.items() if f['runtime'] in RETIRED}
out['runtime_counts'] = dict(collections.Counter(f['runtime'] for f in functions.values()))

# fleet-wide health
out['fleet_health'] = dict(collections.Counter(health(n) for n in names))
out['all_errors_7d'] = sorted(n for n in names if health(n) == 'ALL_ERRORS_7D')
out['scheduled_but_silent_30d'] = sorted(n for n in names if health(n) == 'NO_INVOCATIONS_30D_SCHEDULED')

# ---------------------------------------------------------------- F. capital-structure control
try:
    import capital_structure_refresh as refresh
    import share_structure_campaign as campaign
    state, etag, _ = refresh.load_control(s3)
    cs = {'control_present': state is not None}
    if state:
        cs.update({k: state.get(k) for k in ('contract', 'status', 'request_id', 'run_id', 'started_at',
                                              'completed_at', 'phase', 'phases_complete', 'failed_phase', 'error')
                   if k in state})
        cs['state_keys'] = sorted(state.keys())
    try:
        acc = campaign.read_journal(s3, refresh.ACCEPTED_STATUS)
        cs['accepted_journal'] = {'request_id': acc.get('request_id'), 'status': acc.get('status'),
                                  'counts': acc.get('counts'), 'qualification_conserved':
                                  (acc.get('qualification') or {}).get('all_original_rows_conserved')}
    except Exception as e:
        cs['accepted_journal_error'] = type(e).__name__ + ': ' + str(e)[:120]
    # replicate the plan-phase preconditions to name the failing one
    reasons = []
    if state and state.get('contract') != refresh.CONTRACT:
        reasons.append('contract mismatch: %r' % state.get('contract'))
    if state and state.get('status') != 'complete':
        reasons.append('previous status is %r (needs complete)' % state.get('status'))
    cs['plan_blockers'] = reasons
    out['capital_structure'] = cs
except Exception as e:
    out['capital_structure'] = {'error': type(e).__name__ + ': ' + str(e)[:200]}

# ---------------------------------------------------------------- G. ops queue exposure
try:
    r = subprocess.run([sys.executable, 'scripts/ops_queue.py', 'select', '--base', 'HEAD~1', '--head', 'HEAD'],
                       cwd=ROOT, capture_output=True, text=True, timeout=120)
    picked = r.stdout.split()
    out['ops_queue'] = {'would_run_on_next_pending_push': len(picked), 'sample': picked[:20],
                        'stderr': r.stderr[-400:]}
except Exception as e:
    out['ops_queue'] = {'error': str(e)[:200]}

# ---------------------------------------------------------------- report
lines = ['# ops 6496 - fleet triage (read-only)', '', '- ran_at: %s' % NOW.isoformat(),
         '- functions: %d; classic rules: %d; scheduler schedules: %d' % (len(functions), rule_count, sched_count),
         '- runtime_counts: %s' % out['runtime_counts'], '- fleet_health: %s' % out['fleet_health'],
         '- rowcount_history_days: %d' % rowdays, '', '## Violation summary (class | worst writer health)', '']
for k, n in out['violation_summary'].items():
    lines.append('- %s: %d' % (k, n))
lines += ['', '## Functions failing on every run (7d)', ''] + ['- %s (inv7=%d err7=%d)' % (
    n, functions[n]['inv7'], functions[n]['err7']) for n in out['all_errors_7d']]
lines += ['', '## Scheduled but silent for 30 days', ''] + ['- %s %s' % (
    n, [(s['kind'], s['name'], s['state']) for s in functions[n]['schedules']]) for n in out['scheduled_but_silent_30d']]
lines += ['', '## Retired runtimes', '', '| function | runtime | last_modified | in_repo | inv30 | err30 | schedules |',
          '|---|---|---|---|---|---|---|']
for n, f in sorted(out['retired_runtime_functions'].items()):
    lines.append('| %s | %s | %s | %s | %d | %d | %s |' % (n, f['runtime'], f['last_modified'][:10], f['in_repo'],
                                                        f['inv30'], f['err30'],
                                                        ' '.join('%s:%s' % (s['name'], s['state']) for s in f['schedules'])))
lines += ['', '## Capital-structure refresh control', '', '```json', json.dumps(out['capital_structure'], indent=1, default=str), '```']
lines += ['', '## Ops queue exposure', '', '```json', json.dumps(out['ops_queue'], indent=1), '```']
lines += ['', '## Full data', '', '```json', json.dumps(out, default=str, separators=(',', ':')), '```', '', 'VERDICT: PASS']
REPORT.parent.mkdir(parents=True, exist_ok=True)
REPORT.write_text('\n'.join(lines) + '\n', encoding='utf-8')
print('\n'.join(lines[:60]))
