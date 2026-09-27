"""Exact SEC search/consumer packages, original schedules and producer replay.

No consumer output, account, learning log, notification or provider acquisition.
Native producers are never invoked; only the ordinary search head is read.
"""
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
import sec_search_model as model
import sec_search_store as store

BUCKET = 'justhodl-dashboard-live'
PRODUCER = 'justhodl-sec-filings-intel'
SOURCES = {PRODUCER: 6, 'justhodl-best-ideas': 5,
           'justhodl-convergence-radar': 5, 'justhodl-pump-mechanics': 4}


def check_runtime(before, original, expected, count):
    if before['receipt'] != {'status': 'matched', 'commit': expected} or before['source_files_checked'] != count:
        raise ValueError('Exact complete SEC release required')
    cfg = original['runtime']
    mapping = {'function_name': 'FunctionName', 'runtime': 'Runtime', 'handler': 'Handler', 'timeout': 'Timeout',
               'memory_mb': 'MemorySize', 'architectures': 'Architectures', 'role': 'Role'}
    if (any(before[k] != cfg[v] for k, v in mapping.items())
            or before['ephemeral_storage_mb'] != cfg['EphemeralStorage']['Size']
            or before['schedules'] != original['schedules']):
        raise ValueError('Original runtime or cadence differs')


def publication(s3, native_path):
    found = store.get(s3, BUCKET, model.HEAD, 'search')
    if found is None: raise ValueError('Whole current search packet required')
    raw = found['raw']; packet = model.strict(raw)
    result = {'status': 'pending_original_schedule_publication', 'bytes': len(raw), 'sha256': model.sha(raw),
              'generated_at': packet.get('generated_at'), 'contract': packet.get('contract')}
    protected = []
    if packet.get('contract') == model.CONTRACT:
        result.update(status='complete_native_search_originals_replayed',
                      replay=store.replay(s3, BUCKET, native_path, 'search', packet), quality=packet['quality'],
                      source_responses=packet['source_responses'])
        ref = packet['publication_context']['manifest']; protected.append(ref['key'])
        plan = model.strict(store.retained(s3, BUCKET, ref, 'search'))
        protected.extend(identity['key'] for identity in plan['compilers'].values())
        protected.extend(plan[key]['key'] for key in ('input', 'output'))
        inputs = model.strict(store.retained(s3, BUCKET, plan['input'], 'search'))
        if inputs.get('prior'): protected.append(inputs['prior']['original']['key'])
        for attempt in inputs['attempts']:
            protected.append(attempt['attempt_manifest']['key'])
            if 'original' in attempt: protected.append(attempt['original']['key'])
    return result, protected


def main():
    clients = {name: boto3.client(name, region_name='us-east-1') for name in ('lambda', 's3', 'events', 'scheduler')}
    lam, s3, events, scheduler = (clients[name] for name in ('lambda', 's3', 'events', 'scheduler'))
    producer = json.loads((ROOT / 'docs/audit/2026-09-27/filing-original-baseline.json').read_bytes())
    consumers = json.loads((ROOT / 'docs/audit/2026-09-27/sec-consumer-original-baseline.json').read_bytes())
    originals = {PRODUCER: producer['actual_producers'][PRODUCER], **consumers['actual_consumers']}
    protected = [producer['baseline']['key'], consumers['baseline']['key']]
    results = {}
    with report('ops_6232_sec_search_research_acceptance') as r:
        for fn, count in SOURCES.items():
            native = ROOT / 'aws/lambdas' / fn / 'source/lambda_function.py'
            paths = (['aws/shared/' + name for name in store.COMPILERS[1:]] if fn == PRODUCER
                     else ['aws/shared/sec_search_research.py'])
            expected = subprocess.check_output(['git', 'log', '-1', '--format=%H', '--', 'aws/lambdas/' + fn, *paths], cwd=ROOT, text=True).strip()
            before = runtime(lam, s3, events, scheduler, fn)
            check_runtime(before, originals[fn], expected, count)
            subprocess.run([sys.executable, str(native.parent.parent / 'tests/run_tests.py')], cwd=ROOT, check=True)
            result = {'expected_commit': expected, 'actual_runtime': before}
            if fn == PRODUCER:
                result['native_publication'], extra = publication(s3, native); protected.extend(extra)
            if runtime(lam, s3, events, scheduler, fn) != before:
                raise ValueError('SEC runtime changed during read-only acceptance')
            results[fn] = result
        privacy = access.summarize([access.check(key) for key in sorted(set(protected))])
        if not privacy['all_denied']: raise ValueError('Original research/code must remain private')
        r.kv(engines=results, **privacy, native_invocations=0, provider_requests=0, consumer_output_reads=0,
             account_reads=0, credential_reads=0, learning_log_reads=0, notifications_sent=0,
             public_writes=0, history_writes=0, schedule_changes=0,
             scope='Exact deployed packages, isolated consumer abstention tests and complete naturally scheduled SEC search-response replay. No verified issuer event, ownership concentration, complete SEC universe, historical vintage, forecast edge or portfolio sizing claim.')


if __name__ == '__main__':
    try: main()
    except Exception: sys.exit(1)
