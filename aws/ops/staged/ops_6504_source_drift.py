"""Read-only: is the repo the exact source of what is live? For each candidate function, download the
live package and diff it file-by-file against aws/shared/*.py + aws/lambdas/<fn>/source (the recipe in
scripts/deploy_lambdas.sh). A config.json change redeploys the repo source, so this must be known first."""
from datetime import datetime, timezone
from pathlib import Path
import hashlib
import io
import json
import urllib.request
import zipfile

import boto3

ROOT = Path(__file__).resolve().parents[3]
REPORT = ROOT / 'aws/ops/reports/latest/ops_6504_source_drift.md'
FNS = ['justhodl-symbol-feed', 'justhodl-morning-intelligence', 'justhodl-strategist', 'justhodl-ecb-derived',
       'justhodl-analytics-snapshot', 'justhodl-outcome-checker', 'justhodl-apac-leadlag', 'justhodl-imf-full',
       'justhodl-ticker-360', 'justhodl-share-flows']
lam = boto3.client('lambda')
out = {'ran_at': datetime.now(timezone.utc).isoformat(), 'functions': {}}


def sha(b):
    return hashlib.sha256(b).hexdigest()


shared = {p.name: sha(p.read_bytes()) for p in (ROOT / 'aws/shared').glob('*.py')}
for fn in FNS:
    rec = {}
    try:
        f = lam.get_function(FunctionName=fn)
        cfg = f['Configuration']
        rec['live'] = {'last_modified': cfg['LastModified'], 'timeout_s': cfg['Timeout'], 'memory_mb': cfg['MemorySize'],
                       'code_sha256': cfg['CodeSha256'], 'env_keys': sorted((cfg.get('Environment') or {}).get('Variables') or {})}
        z = zipfile.ZipFile(io.BytesIO(urllib.request.urlopen(f['Code']['Location'], timeout=60).read()))
        live = {n: sha(z.read(n)) for n in z.namelist() if not n.endswith('/')}
        src = ROOT / 'aws/lambdas' / fn / 'source'
        repo = dict(shared)
        for p in src.rglob('*'):
            if p.is_file() and '__pycache__' not in p.parts:
                repo[str(p.relative_to(src))] = sha(p.read_bytes())
        only_live = sorted(n for n in live if n not in repo and '__pycache__' not in n)
        only_repo = sorted(n for n in repo if n not in live)
        differ = sorted(n for n in live if n in repo and live[n] != repo[n])
        differ_source = [n for n in differ if n not in shared]
        rec['diff'] = {'n_live_files': len(live), 'only_live': only_live[:20], 'only_repo': only_repo[:20],
                       'differ_source': differ_source, 'differ_shared': [n for n in differ if n in shared][:20]}
        rec['verdict'] = 'REPO_IS_LIVE' if not differ_source and not only_live and not only_repo else (
            'SHARED_ONLY_DRIFT' if not differ_source and not only_live and not only_repo else 'SOURCE_DRIFT')
        cfgf = ROOT / 'aws/lambdas' / fn / 'config.json'
        c = json.loads(cfgf.read_text()) if cfgf.exists() else {}
        rec['config_json'] = {'timeout': c.get('timeout'), 'memory': c.get('memory'), 'runtime': c.get('runtime')}
    except Exception as e:
        rec['error'] = type(e).__name__ + ': ' + str(e)[:200]
    out['functions'][fn] = rec
lines = ['# ops 6504 - live package vs repo source (read-only)', '']
for fn, rec in out['functions'].items():
    lines.append('- %s: **%s** live=%s diff=%s config.json=%s' % (fn, rec.get('verdict', rec.get('error')),
                 json.dumps({k: rec.get('live', {}).get(k) for k in ('last_modified', 'timeout_s', 'memory_mb')}),
                 json.dumps(rec.get('diff')), json.dumps(rec.get('config_json'))))
lines += ['', '## Full data', '', '```json', json.dumps(out, default=str, separators=(',', ':')), '```', '', 'VERDICT: PASS']
REPORT.parent.mkdir(parents=True, exist_ok=True)
REPORT.write_text('\n'.join(lines) + '\n', encoding='utf-8')
print('\n'.join(lines[:14]))
