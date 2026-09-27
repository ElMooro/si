"""Read-only exact SEC Atom packages, unchanged cadence and retained replay."""
from pathlib import Path
import json
import subprocess
import sys
import boto3

ROOT = Path(__file__).resolve().parents[3]
sys.path[:0] = [str(ROOT / p) for p in ('aws/ops', 'aws/ops/checks', 'aws/shared')]
from ops_report import report
from market_runtime_evidence import runtime
import retained_access_evidence as access
import sec_atom_model as model
import sec_atom_store as store

BUCKET = 'justhodl-dashboard-live'
FAMILIES = {'justhodl-sec-8k': '8k', 'justhodl-sec-10kq': '10kq'}


def check_runtime(before, original, expected):
    if before['receipt'] != {'status': 'matched', 'commit': expected} or before['source_files_checked'] != 3:
        raise ValueError('Exact three-source filing release required')
    cfg = original['runtime']
    mapping = {'function_name': 'FunctionName', 'runtime': 'Runtime', 'handler': 'Handler', 'timeout': 'Timeout',
               'memory_mb': 'MemorySize', 'architectures': 'Architectures', 'role': 'Role'}
    if (any(before[k] != cfg[v] for k, v in mapping.items())
            or before['ephemeral_storage_mb'] != cfg['EphemeralStorage']['Size']
            or before['schedules'] != original['schedules']):
        raise ValueError('Original filing runtime or cadence differs')


def publication(s3, native_path, kind):
    found = store.get(s3, BUCKET, model.HEADS[kind], kind)
    if found is None: raise ValueError('Whole current filing packet required')
    raw = found['raw']; packet = model.strict(raw)
    result = {'status': 'pending_original_schedule_publication', 'bytes': len(raw), 'sha256': model.sha(raw),
              'generated_at': packet.get('generated_at'), 'contract': packet.get('contract')}
    protected = []
    if packet.get('contract') == model.CONTRACT:
        result.update(status='complete_native_atom_originals_replayed',
                      replay=store.replay(s3, BUCKET, native_path, kind, packet), quality=packet['quality'],
                      statistics=packet['stats'], source_responses=packet['source_responses'])
        ref = packet['publication_context']['manifest']; protected.append(ref['key'])
        plan = model.strict(store.retained(s3, BUCKET, ref, kind))
        protected.extend(identity['key'] for identity in plan['compilers'].values())
        protected.extend(plan[key]['key'] for key in ('input', 'output'))
        inputs = model.strict(store.retained(s3, BUCKET, plan['input'], kind))
        if inputs.get('prior'): protected.append(inputs['prior']['original']['key'])
        for attempt in inputs['attempts']:
            protected.append(attempt['attempt_manifest']['key'])
            if 'original' in attempt: protected.append(attempt['original']['key'])
    return result, protected


def main():
    clients = {name: boto3.client(name, region_name='us-east-1') for name in ('lambda', 's3', 'events', 'scheduler')}
    lam, s3, events, scheduler = (clients[name] for name in ('lambda', 's3', 'events', 'scheduler'))
    baseline = json.loads((ROOT / 'docs/audit/2026-09-27/filing-original-baseline.json').read_bytes())
    results = {}; protected = [baseline['baseline']['key']]
    with report('ops_6230_sec_atom_research_acceptance') as r:
        for fn, kind in FAMILIES.items():
            native_path = ROOT / 'aws/lambdas' / fn / 'source/lambda_function.py'
            expected = subprocess.check_output(['git', 'log', '-1', '--format=%H', '--', 'aws/lambdas/' + fn,
                                                'aws/shared/sec_atom_model.py', 'aws/shared/sec_atom_store.py'], cwd=ROOT, text=True).strip()
            before = runtime(lam, s3, events, scheduler, fn)
            check_runtime(before, baseline['actual_producers'][fn], expected)
            subprocess.run([sys.executable, str(native_path.parent.parent / 'tests/run_tests.py')], cwd=ROOT, check=True)
            evidence, paths = publication(s3, native_path, kind); protected.extend(paths)
            if runtime(lam, s3, events, scheduler, fn) != before:
                raise ValueError('Filing runtime changed during acceptance')
            results[fn] = {'expected_commit': expected, 'actual_runtime': before, 'native_publication': evidence}
        privacy = access.summarize([access.check(key) for key in sorted(set(protected))])
        if not privacy['all_denied']: raise ValueError('Original filing research must remain private')
        r.kv(engines=results, **privacy, native_invocations=0, provider_requests=0, account_reads=0,
             credential_reads=0, public_writes=0, history_writes=0, schedule_changes=0,
             scope='Whole acquired SEC Atom responses, returned filing forms, explicit dates, conflict-preserving metadata and reproducible counts. Full filing text, historical vintages, universe completeness, forecast edge and portfolio sizing remain unqualified.')


if __name__ == '__main__':
    try: main()
    except Exception: sys.exit(1)
