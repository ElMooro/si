"""Move every function off retired Lambda runtimes (2026-10-08 audit, item 4).

  python3.9 / python3.10  -> python3.12   (the fleet standard: 861 of 909 functions)
  nodejs18.x              -> nodejs20.x   (source uses only the https core module; no aws-sdk v2)

Safety, per function:
  * configuration-only change (update_function_configuration); code is NOT redeployed,
    so a repo source that drifted from live cannot be shipped by accident.
  * every attached layer is downloaded and scanned; a layer carrying interpreter-specific
    paths (python/lib/python3.9, *.so, *.cpython-39*) blocks the upgrade for that function
    and is reported instead. The public AWSSDKPandas layer is swapped to its Python 3.12 build.
  * waits for LastUpdateStatus == Successful and re-reads the configuration before recording
    the result. Nothing is invoked.

Source compatibility was checked in the repo beforehand: no removed stdlib modules
(distutils, imp, asynchat, cgi...) in any target; datetime.utcnow() is deprecated-not-removed.
"""
from datetime import datetime, timezone
from pathlib import Path
import io
import json
import re
import sys
import time
import urllib.request
import zipfile

import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[3]
REPORT = ROOT / 'aws/ops/reports/latest/ops_6499_retire_old_runtimes.md'
CFG = Config(retries={'max_attempts': 8, 'mode': 'adaptive'}, read_timeout=60)
lam = boto3.client('lambda', config=CFG)
NOW = datetime.now(timezone.utc)
TARGET = {'python3.9': 'python3.12', 'python3.10': 'python3.12', 'nodejs18.x': 'nodejs20.x'}
PANDAS_LAYER_ACCOUNT = '336392948345'
BAD = re.compile(r'python/lib/python3\.(9|10|11)/|\.cpython-3(9|10|11)[-.]|\.so$|\.so\.\d')
out = {'ran_at': NOW.isoformat(), 'functions': {}}
failed = []


def log(m):
    print('[6499] %s' % m, flush=True)


def scan_layer(arn):
    """Return (ok, note, sample) after reading the layer zip's file list."""
    lv = lam.get_layer_version_by_arn(Arn=arn)
    url = lv['Content']['Location']
    data = urllib.request.urlopen(url, timeout=120).read()
    names = zipfile.ZipFile(io.BytesIO(data)).namelist()
    bad = [n for n in names if BAD.search(n)]
    return (not bad, '%d files, %d interpreter-specific' % (len(names), len(bad)), bad[:8],
            lv.get('CompatibleRuntimes'))


def pandas_layer_for_312():
    arn = 'arn:aws:lambda:us-east-1:%s:layer:AWSSDKPandas-Python312' % PANDAS_LAYER_ACCOUNT
    versions = lam.list_layer_versions(LayerName=arn).get('LayerVersions', [])
    if not versions:
        raise RuntimeError('no public AWSSDKPandas-Python312 versions visible')
    return versions[0]['LayerVersionArn']


def wait_settled(name, timeout=240):
    t0 = time.time()
    while time.time() - t0 < timeout:
        c = lam.get_function_configuration(FunctionName=name)
        if c.get('LastUpdateStatus') in (None, 'Successful'):
            return c
        if c.get('LastUpdateStatus') == 'Failed':
            raise RuntimeError('LastUpdateStatus Failed: %s' % c.get('LastUpdateStatusReason'))
        time.sleep(4)
    raise RuntimeError('update did not settle in %ds' % timeout)


functions = []
for page in lam.get_paginator('list_functions').paginate():
    functions += [f for f in page['Functions'] if f.get('Runtime') in TARGET]
log('%d functions on retired runtimes' % len(functions))

for f in sorted(functions, key=lambda x: x['FunctionName']):
    name = f['FunctionName']
    rec = {'before': f['Runtime'], 'target': TARGET[f['Runtime']], 'layers_before': [l['Arn'] for l in f.get('Layers', [])],
           'layer_scan': [], 'status': None}
    out['functions'][name] = rec
    try:
        wait_settled(name)
        new_layers = []
        blocked = False
        for arn in rec['layers_before']:
            if ':layer:AWSSDKPandas-Python' in arn and rec['target'].startswith('python'):
                repl = pandas_layer_for_312()
                rec['layer_scan'].append({'layer': arn, 'action': 'swap', 'to': repl})
                new_layers.append(repl)
                continue
            ok, note, sample, compat = scan_layer(arn)
            rec['layer_scan'].append({'layer': arn, 'ok': ok, 'note': note, 'sample': sample, 'compatible_runtimes': compat})
            if not ok:
                blocked = True
            new_layers.append(arn)
        if blocked:
            rec['status'] = 'BLOCKED_BY_LAYER'
            log('%s blocked by interpreter-specific layer' % name)
            continue
        kw = {'FunctionName': name, 'Runtime': rec['target']}
        if new_layers != rec['layers_before']:
            kw['Layers'] = new_layers
        lam.update_function_configuration(**kw)
        c = wait_settled(name)
        rec['after'] = c['Runtime']
        rec['layers_after'] = [l['Arn'] for l in c.get('Layers', [])]
        rec['last_modified'] = c['LastModified']
        rec['status'] = 'UPGRADED' if c['Runtime'] == rec['target'] else 'MISMATCH'
        if rec['status'] != 'UPGRADED':
            failed.append(name)
        log('%s %s -> %s' % (name, rec['before'], rec['after']))
    except Exception as e:
        rec['status'] = 'ERROR'
        rec['error'] = type(e).__name__ + ': ' + str(e)[:200]
        failed.append(name)
        log('%s ERROR %s' % (name, rec['error']))

remaining = []
for page in lam.get_paginator('list_functions').paginate():
    remaining += [(x['FunctionName'], x['Runtime']) for x in page['Functions'] if x.get('Runtime') in TARGET]
out['remaining_on_retired_runtimes'] = remaining

lines = ['# ops 6499 - retire old Lambda runtimes', '', '- ran_at: %s' % NOW.isoformat(),
         '- targets: %d; upgraded: %d; blocked: %d; errors: %d; remaining on retired runtimes: %d' % (
             len(out['functions']), sum(1 for r in out['functions'].values() if r['status'] == 'UPGRADED'),
             sum(1 for r in out['functions'].values() if r['status'] == 'BLOCKED_BY_LAYER'),
             sum(1 for r in out['functions'].values() if r['status'] == 'ERROR'), len(remaining)),
         '', '| function | before | after | status | layers |', '|---|---|---|---|---|']
for n, r in out['functions'].items():
    lines.append('| %s | %s | %s | %s | %s |' % (n, r['before'], r.get('after', '-'), r['status'],
                                                 '; '.join(s.get('note') or s.get('action', '') for s in r['layer_scan']) or '-'))
lines += ['', '## Full data', '', '```json', json.dumps(out, default=str, separators=(',', ':')), '```', '',
          'VERDICT: %s' % ('FAIL' if failed else 'PASS')]
REPORT.parent.mkdir(parents=True, exist_ok=True)
REPORT.write_text('\n'.join(lines) + '\n', encoding='utf-8')
print('\n'.join(lines[:40]))
if failed:
    sys.exit(1)
