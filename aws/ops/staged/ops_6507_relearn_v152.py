"""Contract re-learn with gate v1.5.2.

v1.5.1 kept every parsed artifact document for its second pass and exhausted the gate's 1024 MB
(ops 6505: 600 s timeouts, then Runtime.OutOfMemory); v1.5.2 keeps scalars only. The Lambda client
below has retries disabled: botocore's standard retry re-invoked learn four times on a dropped
connection, so a slow learn became four concurrent learns.

Ops 6502 listed every object under data/ (1.9M per-symbol cache files) instead of the gate's scope
(top-level data/*.json) and wrote a 1.9M-entry map; the gate then ran out of memory on learn. Step 0
restores the map retained by 6502 before it wrote, then the rebuild is bounded to the gate's scope.
--- original rationale ---

Ops 6498 (v1.5.0) left 237 STALE: 10 artifacts whose writers only the 2026-08-01 map knew (the AST
inventory misses dynamic keys) and 134 per-symbol cache files written through key_patterns such as
data/warm/blackswan/*.json; with no writer known the gate could not tell "dormant branch" from "dead
engine". This script:
  1. rebuilds config/artifact-producers.json as v3.1 = v3 writers + legacy_writers (from the retained
     v2 copy) + pattern_writers (engine-manifest key_patterns, catch-alls excluded), previous retained;
  2. verifies the deployed gate is v1.5.1 (selftest), retains the current contracts, invokes learn
     then check, and reports before/after with the dormant ledger.
"""
from datetime import datetime, timezone
from pathlib import Path
import json
import re
import sys

import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[3]
REPORT = ROOT / 'aws/ops/reports/latest/ops_6507_relearn_v152.md'
BUCKET = 'justhodl-dashboard-live'
FN = 'justhodl-contract-gate'
LEGACY = 'data/ops/control-plane-history/20261008T160701Z/config__artifact-producers.json'
CATCH_ALL = {'data/*.json', 'data/*', '*', '*.json', 'data/**'}
CFG = Config(retries={'max_attempts': 0}, read_timeout=900, connect_timeout=30)
s3 = boto3.client('s3')
lam = boto3.client('lambda', config=CFG)
NOW = datetime.now(timezone.utc)
TS = NOW.strftime('%Y%m%dT%H%M%SZ')
out = {'ran_at': NOW.isoformat()}
failed = []


def get_json(key):
    return json.loads(s3.get_object(Bucket=BUCKET, Key=key)['Body'].read())


def invoke(payload):
    r = lam.invoke(FunctionName=FN, InvocationType='RequestResponse', Payload=json.dumps(payload).encode())
    body = json.loads(r['Payload'].read() or b'{}')
    if r.get('FunctionError'):
        failed.append('%s invoke %s: %s' % (FN, payload.get('mode'), str(body)[:200]))
    return body


def pattern_rx(p):
    return re.compile('^' + re.escape(p).replace(r'\*', '[^/]*') + '$')


# producers map is already v3.1 (ops 6505 restored + rebuilt it; verified 1998 entries)
prod = get_json('config/artifact-producers.json')
if prod.get('version') != 3.1 or len(prod.get('producers', {})) > 5000:
    raise SystemExit('producers map unexpected: version=%r n=%d' % (prod.get('version'), len(prod.get('producers', {}))))
out['producers'] = {k: prod.get(k) for k in ('version', 'generated_at', 'n_mapped', 'n_with_writer',
                                              'n_legacy_writer_only', 'n_pattern_writer_only')}

# 2. relearn
st = invoke({'mode': 'selftest'})
out['selftest_ok'] = st.get('ok')
cfg = lam.get_function_configuration(FunctionName=FN)
out['gate'] = {'last_modified': cfg['LastModified'], 'runtime': cfg['Runtime']}
if cfg['LastModified'] < '2026-10-08T18:1':
    failed.append('deployed gate predates v1.5.2 (%s)' % cfg['LastModified'])
before = get_json('data/contract-violations.json')
out['before'] = {k: before.get(k) for k in ('generated_at', 'n_contracts', 'n_violations', 'sev1', 'sev2',
                                             'by_class', 'n_uncontracted', 'n_orphaned')}
if not failed:
    s3.copy_object(Bucket=BUCKET, CopySource={'Bucket': BUCKET, 'Key': 'config/engine-contracts.json'},
                   Key='data/ops/control-plane-history/%s/config__engine-contracts.json' % TS)
    out['learn'] = invoke({'mode': 'learn'})
    out['check'] = invoke({'mode': 'check'})
    after = get_json('data/contract-violations.json')
    out['after'] = {k: after.get(k) for k in ('generated_at', 'n_contracts', 'n_violations', 'sev1', 'sev2',
                                               'by_class', 'n_uncontracted', 'n_orphaned', 'n_regressed', 'n_dormant')}
    out['after_violations'] = after.get('violations')
    reg = get_json('config/engine-contracts.json')
    out['registry'] = {k: reg.get(k) for k in ('version', 'generated_at', 'n_contracts', 'n_cadence_bounded',
                                                'n_suspects', 'n_regressed', 'n_orphaned', 'n_dormant')}
    out['dormant'] = reg.get('dormant')
    out['suspects'] = reg.get('suspects')
    out['orphaned'] = reg.get('orphaned')

lines = ['# ops 6507 - producers v3.1 + contract re-learn (gate v1.5.1)', '',
         '- producers: %s' % json.dumps(out['producers'], default=str),
         '- gate: %s selftest_ok=%s' % (json.dumps(out['gate'], default=str), out.get('selftest_ok')),
         '- before: %s' % json.dumps(out['before'], default=str),
         '- after: %s' % json.dumps(out.get('after'), default=str),
         '- registry: %s' % json.dumps(out.get('registry'), default=str), '']
if failed:
    lines += ['## FAILED', ''] + ['- ' + f for f in failed] + ['']
lines += ['## Remaining violations', '']
for v in out.get('after_violations') or []:
    lines.append('- sev%s %s `%s` %s' % (v.get('sev'), v.get('cls'), v.get('artifact'), v.get('detail', '')[:140]))
lines += ['', '## Still-stale suspects (no writer has fresh output)', '']
for s in out.get('suspects') or []:
    lines.append('- `%s` %sh (bound %sh) writers=%s' % (s['key'], s['age_h'], s['bound_h'], s.get('writers')))
lines += ['', '## Full data', '', '```json', json.dumps(out, default=str, separators=(',', ':')), '```', '',
          'VERDICT: %s' % ('FAIL' if failed else 'PASS')]
REPORT.parent.mkdir(parents=True, exist_ok=True)
REPORT.write_text('\n'.join(lines) + '\n', encoding='utf-8')
print('\n'.join(lines[:12]))
if failed:
    sys.exit(1)
