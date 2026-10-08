"""Re-learn engine contracts with contract-gate v1.5.0, then run a check (2026-10-08 audit).

Preconditions verified here: the deployed gate reports VERSION 1.5.0 via its selftest
mode (so a stale deploy cannot re-learn with the old key policy), and ops 6497 has
refreshed config/schedule-manifest.json and config/artifact-producers.json today.

The previous registry is retained under data/ops/control-plane-history/<ts>/ before the
learn overwrites config/engine-contracts.json. The only invocations are of the gate
itself (a monitor, never a producer): selftest -> learn -> check.
"""
from datetime import datetime, timedelta, timezone
from pathlib import Path
import collections
import json
import sys

import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[3]
REPORT = ROOT / 'aws/ops/reports/latest/ops_6498_contract_gate_relearn.md'
BUCKET = 'justhodl-dashboard-live'
FN = 'justhodl-contract-gate'
CFG = Config(retries={'max_attempts': 3, 'mode': 'standard'}, read_timeout=900, connect_timeout=30)
s3 = boto3.client('s3')
lam = boto3.client('lambda', config=CFG)
cw = boto3.client('cloudwatch')
logs = boto3.client('logs')
NOW = datetime.now(timezone.utc)
TS = NOW.strftime('%Y%m%dT%H%M%SZ')
out = {'ran_at': NOW.isoformat()}
failed = []


def invoke(payload):
    r = lam.invoke(FunctionName=FN, InvocationType='RequestResponse', Payload=json.dumps(payload).encode())
    body = json.loads(r['Payload'].read() or b'{}')
    if r.get('FunctionError'):
        raise RuntimeError('%s: %s' % (payload, json.dumps(body)[:400]))
    return body


def get_json(key):
    return json.loads(s3.get_object(Bucket=BUCKET, Key=key)['Body'].read())


def head_age_h(key):
    h = s3.head_object(Bucket=BUCKET, Key=key)
    return (NOW - h['LastModified']).total_seconds() / 3600


# preconditions
cfg = lam.get_function_configuration(FunctionName=FN)
out['deployed'] = {'last_modified': cfg['LastModified'], 'runtime': cfg['Runtime'], 'timeout': cfg['Timeout']}
st = invoke({'mode': 'selftest'})
out['selftest'] = st
names = {c['case'] for c in st.get('cases', [])}
if not st.get('ok') or 'stable_keys_intersect' not in names:
    failed.append('deployed gate is not v1.5.0 (selftest cases: %s)' % sorted(names))
for key in ('config/schedule-manifest.json', 'config/artifact-producers.json'):
    age = head_age_h(key)
    out['input_age_h_' + key.split('/')[-1]] = round(age, 2)
    if age > 24:
        failed.append('%s is %.0fh old; run ops 6497 first' % (key, age))

before = get_json('data/contract-violations.json')
out['before'] = {k: before.get(k) for k in ('generated_at', 'n_contracts', 'n_violations', 'sev1', 'sev2',
                                             'by_class', 'n_uncontracted')}
if not failed:
    s3.copy_object(Bucket=BUCKET, CopySource={'Bucket': BUCKET, 'Key': 'config/engine-contracts.json'},
                   Key='data/ops/control-plane-history/%s/config__engine-contracts.json' % TS)
    out['learn'] = invoke({'mode': 'learn'})
    out['check'] = invoke({'mode': 'check'})
    after = get_json('data/contract-violations.json')
    out['after'] = {k: after.get(k) for k in ('generated_at', 'n_contracts', 'n_violations', 'sev1', 'sev2',
                                               'by_class', 'n_uncontracted', 'n_orphaned', 'n_regressed')}
    out['after_violations'] = after.get('violations')
    reg = get_json('config/engine-contracts.json')
    out['registry'] = {k: reg.get(k) for k in ('version', 'generated_at', 'n_contracts', 'n_cadence_bounded',
                                                'rowcount_history_days', 'n_suspects', 'n_regressed', 'n_orphaned')}
    out['regressed'] = reg.get('regressed')
    out['orphaned'] = reg.get('orphaned')
    out['suspects'] = reg.get('suspects')

# first run of the new directory writer (its acceptance test), then 7-day error-log capture
try:
    r = lam.invoke(FunctionName='justhodl-engine-registry', InvocationType='RequestResponse', Payload=b'{}')
    out['engine_registry'] = json.loads(r['Payload'].read() or b'{}')
    if r.get('FunctionError'):
        out['engine_registry']['FunctionError'] = r['FunctionError']
except Exception as e:
    out['engine_registry'] = {'error': type(e).__name__ + ': ' + str(e)[:200]}

names = []
for page in lam.get_paginator('list_functions').paginate():
    names += [f['FunctionName'] for f in page['Functions']]
names.sort()
start = NOW - timedelta(days=7)
q = []
for i, n in enumerate(names):
    for stat in ('Invocations', 'Errors'):
        q.append({'Id': 'q%d_%s' % (i, stat[0]), 'MetricStat': {'Metric': {
            'Namespace': 'AWS/Lambda', 'MetricName': stat, 'Dimensions': [{'Name': 'FunctionName', 'Value': n}]},
            'Period': 604800, 'Stat': 'Sum'}})
m = collections.defaultdict(lambda: {'inv7': 0, 'err7': 0})
for i in range(0, len(q), 500):
    token = None
    while True:
        kw = {'MetricDataQueries': q[i:i + 500], 'StartTime': start, 'EndTime': NOW}
        if token:
            kw['NextToken'] = token
        resp = cw.get_metric_data(**kw)
        for r in resp['MetricDataResults']:
            idx, stt = r['Id'][1:].split('_')
            m[names[int(idx)]]['inv7' if stt == 'I' else 'err7'] += sum(r['Values'])
        token = resp.get('NextToken')
        if not token:
            break
out['erroring_7d'] = {}
for n, v in sorted(((n, v) for n, v in m.items() if v['err7'] > 0), key=lambda kv: -kv[1]['err7']):
    rec = {'inv7': v['inv7'], 'err7': v['err7'], 'lines': []}
    try:
        evs = logs.filter_log_events(
            logGroupName='/aws/lambda/' + n, startTime=int(start.timestamp() * 1000), limit=80,
            filterPattern='?Traceback ?"[ERROR]" ?"Task timed out" ?ImportModuleError ?errorMessage ?errorType').get('events', [])
        rec['lines'] = [e['message'].rstrip()[:500] for e in evs[-16:]]
    except Exception as e:
        rec['lines'] = ['LOG READ FAILED: %s' % str(e)[:120]]
    out['erroring_7d'][n] = rec

lines = ['# ops 6498 - contract gate re-learn (v1.5.0)', '', '- ran_at: %s' % NOW.isoformat(),
         '- deployed gate: %s' % json.dumps(out['deployed']),
         '- selftest ok: %s (%d cases)' % (st.get('ok'), len(st.get('cases', []))),
         '- before: %s' % json.dumps(out['before']), '']
if failed:
    lines += ['## BLOCKED', ''] + ['- %s' % f for f in failed]
else:
    lines += ['- learn: %s' % json.dumps(out['learn']), '- check: %s' % json.dumps(out['check']),
              '- after: %s' % json.dumps(out['after']), '- registry: %s' % json.dumps(out['registry']), '',
              '## Remaining violations', '', '| sev | class | artifact | detail |', '|---|---|---|---|']
    for v in out['after_violations'] or []:
        lines.append('| %s | %s | %s | %s |' % (v['sev'], v['cls'], v['artifact'], v['detail'][:120].replace('|', '/')))
    lines += ['', '## Regressed (contracted at today\'s floor, listed for review)', '',
              '| artifact | rows now | history max | previous learned | writers |', '|---|---|---|---|---|']
    for r in out['regressed'] or []:
        lines.append('| %s | %s | %s | %s | %s |' % (r['key'], r['rows_now'], r['history_max_rows'],
                                                    r.get('previous_learned_rows'), ' '.join(r.get('writers') or [])))
    lines += ['', '## Orphaned (no writer, >30 days silent; not contracted)', '', '| artifact | age_h | readers |', '|---|---|---|']
    for o in out['orphaned'] or []:
        lines.append('| %s | %s | %s |' % (o['key'], o['age_h'], ' '.join(o.get('readers') or [])))
lines += ['', '## engine-registry first run', '', '```json', json.dumps(out.get('engine_registry'), indent=1), '```',
          '', '## Functions with errors in the last 7 days', '']
for n, rec in out['erroring_7d'].items():
    lines += ['### %s (inv7=%d err7=%d)' % (n, rec['inv7'], rec['err7']), '```'] + (rec['lines'] or ['(no matching log lines)']) + ['```']
lines += ['', '## Full data', '', '```json', json.dumps(out, default=str, separators=(',', ':')), '```', '',
          'VERDICT: %s' % ('FAIL' if failed else 'PASS')]
REPORT.parent.mkdir(parents=True, exist_ok=True)
REPORT.write_text('\n'.join(lines) + '\n', encoding='utf-8')
print('\n'.join(lines[:14]))
if failed:
    sys.exit(1)
