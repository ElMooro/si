"""Refresh the three stale control-plane inputs the contract gate depends on (2026-10-08 audit).

  1. config/schedule-manifest.json  -- regenerated from LIVE EventBridge rules + Scheduler
     schedules. The reconciler (mode enforce-duplicates) reported 359 drifts against the
     2026-09-13 manifest; declared state is re-based on reality so drift means change again.
  2. config/artifact-producers.json -- regenerated from the checked-in engine-manifest.json
     (gen_engine_manifest.py, AST write/read inventory). The 2026-08-01 map left 77 contracted
     artifacts with no writer and mis-bounded cadences.
  3. IAM read check for the new justhodl-engine-registry writer (lambda/events/scheduler list).

Also captures, read-only, the last error lines for every function that errored in the past
7 days, so the real engine failures can be fixed from source.

Previous copies of (1) and (2) are retained under data/ops/control-plane-history/<ts>/
before overwrite. No Lambda is invoked; no schedule is created, changed or removed.
"""
from datetime import datetime, timedelta, timezone
from pathlib import Path
import collections
import json
import subprocess
import sys

import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[3]
REPORT = ROOT / 'aws/ops/reports/latest/ops_6497_control_plane_refresh.md'
BUCKET = 'justhodl-dashboard-live'
CFG = Config(retries={'max_attempts': 10, 'mode': 'adaptive'}, read_timeout=60)
s3 = boto3.client('s3', config=CFG)
lam = boto3.client('lambda', config=CFG)
cw = boto3.client('cloudwatch', config=CFG)
logs = boto3.client('logs', config=CFG)
ev = boto3.client('events', config=CFG)
sch = boto3.client('scheduler', config=CFG)
iam = boto3.client('iam', config=CFG)
NOW = datetime.now(timezone.utc)
TS = NOW.strftime('%Y%m%dT%H%M%SZ')
out = {'ran_at': NOW.isoformat(), 'notes': []}
failures = []


def log(m):
    print('[6497] %s' % m, flush=True)


def fn_name(arn):
    return arn.split(':function:')[1].split(':')[0] if isinstance(arn, str) and ':function:' in arn else None


def retain(key):
    try:
        s3.copy_object(Bucket=BUCKET, CopySource={'Bucket': BUCKET, 'Key': key},
                       Key='data/ops/control-plane-history/%s/%s' % (TS, key.replace('/', '__')),
                       MetadataDirective='COPY')
        return True
    except Exception as e:
        out['notes'].append('retain %s: %s' % (key, str(e)[:100]))
        return False


def put_json(key, doc):
    body = json.dumps(doc, indent=1, sort_keys=False).encode()
    s3.put_object(Bucket=BUCKET, Key=key, Body=body, ContentType='application/json', CacheControl='max-age=300')
    back = s3.get_object(Bucket=BUCKET, Key=key)['Body'].read()
    if back != body:
        raise RuntimeError('readback differs for %s' % key)
    return len(body)


# ------------------------------------------------------------------ 1. schedule manifest from live
old_manifest = json.loads(s3.get_object(Bucket=BUCKET, Key='config/schedule-manifest.json')['Body'].read())
rules = []
for page in ev.get_paginator('list_rules').paginate():
    for r in page['Rules']:
        if not r.get('ScheduleExpression'):
            continue  # event-pattern rules are not schedules
        targets = []
        try:
            for t in ev.list_targets_by_rule(Rule=r['Name']).get('Targets', []):
                targets.append({'arn': t.get('Arn'), 'id': t.get('Id'), 'input': t.get('Input'),
                                'path': t.get('InputPath')})
        except Exception as e:
            out['notes'].append('targets %s: %s' % (r['Name'], str(e)[:80]))
        rules.append({'expr': r['ScheduleExpression'], 'kind': 'events', 'name': r['Name'],
                      'state': r.get('State'), 'targets': targets})
schedules = []
for page in sch.get_paginator('list_schedules').paginate():
    for s in page['Schedules']:
        try:
            g = sch.get_schedule(Name=s['Name'], GroupName=s.get('GroupName', 'default'))
        except Exception as e:
            out['notes'].append('get_schedule %s: %s' % (s['Name'], str(e)[:80]))
            continue
        t = g.get('Target') or {}
        schedules.append({'expr': g.get('ScheduleExpression'), 'group': g.get('GroupName'), 'kind': 'scheduler',
                          'name': g['Name'], 'state': g.get('State'),
                          'timezone': g.get('ScheduleExpressionTimezone'),
                          'targets': [{'arn': t.get('Arn'), 'input': t.get('Input'), 'path': None}]})
rules.sort(key=lambda r: r['name'])
schedules.sort(key=lambda r: (r['group'] or '', r['name']))
new_manifest = {
    'doctrine': old_manifest.get('doctrine'),
    'generated_at': NOW.isoformat(),
    'source': 'ops 6497: regenerated from live account state (%d classic rules, %d Scheduler schedules); '
              'previous declaration %s retained under data/ops/control-plane-history/%s/'
              % (len(rules), len(schedules), old_manifest.get('generated_at'), TS),
    'version': 1, 'rules': rules, 'schedules': schedules,
}
old_names = {r['name'] for r in old_manifest.get('rules', []) + old_manifest.get('schedules', [])}
new_names = {r['name'] for r in rules + schedules}
out['manifest'] = {'old_generated_at': old_manifest.get('generated_at'),
                   'old_count': len(old_names), 'new_count': len(new_names),
                   'added': sorted(new_names - old_names)[:400], 'removed': sorted(old_names - new_names)[:400],
                   'live_states': dict(collections.Counter(r['state'] for r in rules + schedules))}
retain('config/schedule-manifest.json')
out['manifest']['bytes'] = put_json('config/schedule-manifest.json', new_manifest)
log('manifest: %d -> %d declarations' % (len(old_names), len(new_names)))

# ------------------------------------------------------------------ 2. artifact producers from source
try:
    subprocess.run([sys.executable, 'scripts/gen_engine_manifest.py'], cwd=ROOT, check=True,
                   capture_output=True, text=True, timeout=600)
except Exception as e:
    out['notes'].append('gen_engine_manifest: %s (using checked-in copy)' % str(e)[:120])
em = json.loads((ROOT / 'engine-manifest.json').read_text(encoding='utf-8'))
engines = em['engines']
engines = engines.values() if isinstance(engines, dict) else engines
old_prod = json.loads(s3.get_object(Bucket=BUCKET, Key='config/artifact-producers.json')['Body'].read())
producers = {}
for e in engines:
    name = e.get('engine')
    for k in e.get('keys') or []:
        producers.setdefault(k, {'writers': [], 'readers': [], 'mentions': []})
        if name not in producers[k]['writers']:
            producers[k]['writers'].append(name)
    for k in e.get('reads') or []:
        producers.setdefault(k, {'writers': [], 'readers': [], 'mentions': []})
        if name not in producers[k]['readers']:
            producers[k]['readers'].append(name)
# keep historical mention-only knowledge for keys the AST pass no longer sees
for k, rec in (old_prod.get('producers') or {}).items():
    if k not in producers:
        producers[k] = {'writers': [], 'readers': [], 'mentions': list(rec.get('mentions') or []),
                        'legacy_writers_2026_08_01': list(rec.get('writers') or [])}
for rec in producers.values():
    rec['writers'].sort()
    rec['readers'].sort()
new_prod = {
    'version': 3, 'generated_at': NOW.isoformat(),
    'schema': 'producers[key] = {writers, readers, mentions}; writers/readers from engine-manifest.json '
              '(%s, AST write/read inventory, %d engines); mentions carried from the v2 map only for keys '
              'the inventory no longer sees' % (em.get('schema_version'), em.get('n_engines')),
    'engine_manifest_generated_at': em.get('generated_at'),
    'n_mapped': len(producers),
    'n_with_writer': sum(1 for r in producers.values() if r['writers']),
    'n_reader_only': sum(1 for r in producers.values() if r['readers'] and not r['writers']),
    'n_mention_only': sum(1 for r in producers.values() if not r['writers'] and not r['readers']),
    'producers': dict(sorted(producers.items())),
}
ow = {k for k, r in (old_prod.get('producers') or {}).items() if r.get('writers')}
nw = {k for k, r in producers.items() if r['writers']}
out['producers'] = {'old': {k: old_prod.get(k) for k in ('generated_at', 'n_mapped', 'n_with_writer')},
                    'new': {k: new_prod[k] for k in ('n_mapped', 'n_with_writer', 'n_reader_only', 'n_mention_only')},
                    'gained_writer': sorted(nw - ow)[:300], 'lost_writer': sorted(ow - nw)[:300]}
retain('config/artifact-producers.json')
out['producers']['bytes'] = put_json('config/artifact-producers.json', new_prod)
log('producers: with_writer %s -> %d' % (old_prod.get('n_with_writer'), new_prod['n_with_writer']))

# ------------------------------------------------------------------ 3. IAM read check for the registry writer
role = 'lambda-execution-role'
want = {'lambda:ListFunctions', 'events:ListRules', 'events:ListTargetsByRule', 'scheduler:ListSchedules',
        's3:PutObject', 's3:GetObject'}
seen = set()
try:
    docs = []
    for p in iam.list_attached_role_policies(RoleName=role)['AttachedPolicies']:
        v = iam.get_policy(PolicyArn=p['PolicyArn'])['Policy']['DefaultVersionId']
        docs.append((p['PolicyName'], iam.get_policy_version(PolicyArn=p['PolicyArn'], VersionId=v)['PolicyVersion']['Document']))
    for n in iam.list_role_policies(RoleName=role)['PolicyNames']:
        docs.append((n, iam.get_role_policy(RoleName=role, PolicyName=n)['PolicyDocument']))
    for name, d in docs:
        for st in d.get('Statement', []):
            if st.get('Effect') != 'Allow':
                continue
            acts = st.get('Action', [])
            acts = [acts] if isinstance(acts, str) else acts
            for a in acts:
                seen.add(a)
    def allowed(action):
        svc = action.split(':')[0]
        return any(a == '*' or a == action or a == svc + ':*' or
                   (a.endswith('*') and action.startswith(a[:-1])) for a in seen)
    out['iam'] = {'role': role, 'policies': [n for n, _ in docs], 'allowed': {a: allowed(a) for a in sorted(want)}}
except Exception as e:
    out['iam'] = {'role': role, 'error': type(e).__name__ + ': ' + str(e)[:160]}
log('iam: %s' % out['iam'].get('allowed', out['iam'].get('error')))

# ------------------------------------------------------------------ 4. error logs for erroring functions (read-only)
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
            idx, st = r['Id'][1:].split('_')
            m[names[int(idx)]]['inv7' if st == 'I' else 'err7'] += sum(r['Values'])
        token = resp.get('NextToken')
        if not token:
            break
erroring = {n: v for n, v in m.items() if v['err7'] > 0}
out['erroring_7d'] = {}
for n, v in sorted(erroring.items(), key=lambda kv: -kv[1]['err7']):
    rec = {'inv7': v['inv7'], 'err7': v['err7'], 'lines': []}
    try:
        resp = logs.filter_log_events(logGroupName='/aws/lambda/' + n, startTime=int(start.timestamp() * 1000),
                                      filterPattern='?Traceback ?"[ERROR]" ?"Task timed out" ?"Runtime.ImportModuleError" ?"errorMessage"',
                                      limit=60)
        evs = resp.get('events', [])
        # keep the tail: the most recent failure is the one worth reading
        for e in evs[-14:]:
            rec['lines'].append(e['message'].rstrip()[:400])
    except Exception as e:
        rec['lines'].append('LOG READ FAILED: %s' % str(e)[:120])
    out['erroring_7d'][n] = rec
log('%d functions errored in 7d' % len(erroring))

# ------------------------------------------------------------------ report
lines = ['# ops 6497 - control-plane refresh', '', '- ran_at: %s' % NOW.isoformat(),
         '- schedule manifest: %d -> %d declarations (added %d, removed %d); live states %s' % (
             out['manifest']['old_count'], out['manifest']['new_count'], len(out['manifest']['added']),
             len(out['manifest']['removed']), out['manifest']['live_states']),
         '- artifact producers: with_writer %s -> %d of %d keys (gained %d, lost %d)' % (
             out['producers']['old'].get('n_with_writer'), out['producers']['new']['n_with_writer'],
             out['producers']['new']['n_mapped'], len(out['producers']['gained_writer']), len(out['producers']['lost_writer'])),
         '- iam: %s' % json.dumps(out['iam'].get('allowed', out['iam'])),
         '', '## Functions with errors in the last 7 days', '']
for n, rec in out['erroring_7d'].items():
    lines.append('### %s (inv7=%d err7=%d)' % (n, rec['inv7'], rec['err7']))
    lines.append('```')
    lines += rec['lines'] or ['(no matching log lines)']
    lines.append('```')
lines += ['', '## Full data', '', '```json', json.dumps(out, default=str, separators=(',', ':')), '```', '',
          'VERDICT: %s' % ('PASS' if not failures else 'FAIL')]
REPORT.parent.mkdir(parents=True, exist_ok=True)
REPORT.write_text('\n'.join(lines) + '\n', encoding='utf-8')
print('\n'.join(lines[:12]))
if failures:
    sys.exit(1)
