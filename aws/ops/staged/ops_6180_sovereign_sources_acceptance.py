"""Read actual sovereign package and any normal source publication; never invoke."""
from pathlib import Path
from datetime import datetime, timezone
import ast
import subprocess
import sys
import boto3
ROOT = Path(__file__).resolve().parents[3]
sys.path[:0] = [str(ROOT / p) for p in ('aws/ops', 'aws/ops/checks', 'aws/lambdas/justhodl-global-sovereign/source')]
from ops_report import report
from market_runtime_evidence import runtime
import sovereign_history as storage
import sovereign_sources as sources
FN = 'justhodl-global-sovereign'
BUCKET = 'justhodl-dashboard-live'


def main():
    lam, s3, events, scheduler = (boto3.client(n, region_name='us-east-1') for n in ('lambda', 's3', 'events', 'scheduler'))
    with report('ops_6180_sovereign_sources_acceptance') as r:
        expected = subprocess.check_output(['git', 'log', '-1', '--format=%H', '--', 'aws/lambdas/' + FN + '/source/sovereign_sources.py'], cwd=ROOT, text=True).strip()
        before = runtime(lam, s3, events, scheduler, FN)
        if before['receipt'] != {'status': 'matched', 'commit': expected} or (before['memory_mb'], before['timeout']) != (512, 300):
            raise ValueError('Exact guarded package and original runtime required')
        if before['schedules'] != [{'kind': 'EventBridge rule', 'name': 'global-sovereign-12h', 'state': 'ENABLED', 'expression': 'cron(15 6,18 * * ? *)', 'native_targets': 1}]:
            raise ValueError('Cadence changed')
        subprocess.run([sys.executable, str(ROOT / 'aws/lambdas' / FN / 'tests/run_tests.py')], cwd=ROOT, check=True)
        state = storage.begin(s3, BUCKET, datetime.now(timezone.utc).isoformat())
        packet = (state['head'] or {}).get('doc', {})
        result = {'status': 'pending_original_scheduled_acquisition'}
        if packet.get('source_evidence'):
            tree = ast.parse((ROOT / 'aws/lambdas' / FN / 'source/lambda_function.py').read_text(encoding='utf-8'))
            countries = next(ast.literal_eval(n.value) for n in tree.body if isinstance(n, ast.Assign) and any(isinstance(t, ast.Name) and t.id == 'COUNTRIES' for t in n.targets))
            def original(key):
                if not key.startswith(storage.PRIVATE):
                    raise ValueError('Only retained sovereign originals allowed')
                return storage.bounded(s3.get_object(Bucket=BUCKET, Key=key)['Body'])
            result = {'status': 'direct_provider_fields_replayed', **sources.verify_publication(packet, original, countries)}
        if runtime(lam, s3, events, scheduler, FN) != before:
            raise ValueError('Runtime changed during verification')
        r.kv(expected_commit=expected, actual_runtime=before, compiler_sha256=sources.compiler_hashes(),
             publication={'generated_at': packet.get('generated_at'), 'version': packet.get('version'), 'whole_bytes': len(state['head']['raw']) if state['head'] else None,
                          'whole_sha256': storage.sha(state['head']['raw']) if state['head'] else None},
             source_verification=result, preserved_history_rows=len((state['history'] or {}).get('doc', [])),
             native_invocations=0, provider_requests=0, archive_writes=0, public_writes=0, history_writes=0,
             account_reads=0, notifications_sent=0, schedules_changed=0,
             scope='Exact actual source package and retained direct provider fields when naturally published. No model, source-definition, quote-clock or portfolio qualification.')


if __name__ == '__main__':
    try:
        main()
    except Exception:
        sys.exit(1)
