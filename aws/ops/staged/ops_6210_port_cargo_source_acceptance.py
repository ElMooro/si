"""Read-only actual code and scheduled complete-source replay; never invoke."""
from pathlib import Path
import json, subprocess, sys
import boto3
ROOT = Path(__file__).resolve().parents[3]
sys.path[:0] = [str(ROOT/p) for p in ('aws/ops', 'aws/ops/checks', 'aws/lambdas/justhodl-port-cargo/source', 'aws/shared')]
from ops_report import report
from market_runtime_evidence import runtime
import cargo_store as store
import lambda_function as native
FN = 'justhodl-port-cargo'; BUCKET = 'justhodl-dashboard-live'


def main():
    lam, s3, events, scheduler = (boto3.client(name, region_name='us-east-1') for name in ('lambda', 's3', 'events', 'scheduler'))
    with report('ops_6210_port_cargo_source_acceptance') as r:
        expected = subprocess.check_output(['git', 'log', '-1', '--format=%H', '--', 'aws/lambdas/'+FN], cwd=ROOT, text=True).strip()
        before = runtime(lam, s3, events, scheduler, FN)
        if before['receipt'] != {'status': 'matched', 'commit': expected}:
            raise ValueError('Exact intended release receipt required')
        baseline = json.loads((ROOT/'docs/audit/2026-09-27/shipping-consumer-baseline.json').read_bytes())['consumer_runtimes'][FN]
        for old, new in {'Runtime': 'runtime', 'Handler': 'handler', 'MemorySize': 'memory_mb', 'Timeout': 'timeout', 'Architectures': 'architectures', 'Role': 'role'}.items():
            if baseline['runtime'][old] != before[new]: raise ValueError('Original runtime differs: '+old)
        if baseline['runtime']['EphemeralStorage']['Size'] != before['ephemeral_storage_mb'] or baseline['schedules'] != before['schedules']:
            raise ValueError('Original storage or schedule differs')
        subprocess.run([sys.executable, str(ROOT/'aws/lambdas'/FN/'tests/run_tests.py')], cwd=ROOT, check=True)
        obj = s3.get_object(Bucket=BUCKET, Key=store.HEAD)
        raw = store.whole(obj['Body'], obj.get('ContentLength')); packet = store.decode(raw)
        publication = {'status': 'pending_original_daily_1240_publication', 'generated_at': packet.get('generated_at'),
                       'version': packet.get('version'), 'whole_bytes': len(raw), 'whole_sha256': store.sha(raw)}
        if packet.get('contract') == store.CONTRACT:
            publication.update(status='whole_native_original_response_calculation_replayed', replay=store.replay(native, s3, BUCKET, packet))
        if runtime(lam, s3, events, scheduler, FN) != before: raise ValueError('Runtime changed during verification')
        r.kv(expected_commit=expected, actual_runtime=before, compiler_sha256=store.hashes(), native_publication=publication,
             native_invocations=0, provider_requests=0, archive_writes=0, public_writes=0, schedule_changes=0, account_reads=0,
             scope='Whole five stored inputs, complete provider responses and exact native calculation replay. Legacy pagination, date windows, missingness, tonnage interpretation, seasonal adjustment and investment impacts remain unqualified.')


if __name__ == '__main__':
    try: main()
    except Exception: sys.exit(1)
