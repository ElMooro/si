"""Read-only actual macro package, original schedule and whole-source replay."""
from pathlib import Path
import json, subprocess, sys
import boto3

ROOT = Path(__file__).resolve().parents[3]
sys.path[:0] = [str(ROOT / p) for p in ('aws/ops', 'aws/ops/checks', 'aws/lambdas/justhodl-macro-leads/source', 'aws/shared')]
from ops_report import report
from market_runtime_evidence import runtime
import macro_store as store
import lambda_function as native
import retained_access_evidence as access

FN = 'justhodl-macro-leads'
BUCKET = 'justhodl-dashboard-live'


def main():
    lam, s3, events, scheduler = (boto3.client(n, region_name='us-east-1') for n in ('lambda', 's3', 'events', 'scheduler'))
    with report('ops_6222_macro_leads_acceptance') as r:
        expected = subprocess.check_output(['git', 'log', '-1', '--format=%H', '--', 'aws/lambdas/' + FN], cwd=ROOT, text=True).strip()
        actual = runtime(lam, s3, events, scheduler, FN)
        r.kv(actual_runtime_before_validation=actual)
        if actual['receipt'] != {'status': 'matched', 'commit': expected} or actual['source_files_checked'] != 20:
            raise ValueError('Exact twenty-source release required')
        accepted = json.loads((ROOT / 'docs/audit/2026-09-27/freight-original-baseline.json').read_bytes())
        original = accepted['consumer_runtimes'][FN]; cfg = original['runtime']
        mapping = {'function_name': 'FunctionName', 'runtime': 'Runtime', 'handler': 'Handler', 'timeout': 'Timeout', 'memory_mb': 'MemorySize', 'architectures': 'Architectures', 'role': 'Role'}
        if any(actual[k] != cfg[v] for k, v in mapping.items()) or actual['ephemeral_storage_mb'] != cfg['EphemeralStorage']['Size'] or actual['schedules'] != original['schedules']:
            raise ValueError('Original macro runtime or cadence differs')
        subprocess.run([sys.executable, str(ROOT / 'aws/lambdas' / FN / 'tests/run_tests.py')], cwd=ROOT, check=True)
        found = store.get(s3, BUCKET, store.HEAD)
        if found is None:
            raise ValueError('Stored macro publication absent')
        packet = store.strict(found['raw'])
        publication = {'status': 'pending_original_1220_publication', 'generated_at': packet.get('generated_at'), 'version': packet.get('version'),
                       'bytes': len(found['raw']), 'sha256': store.sha(found['raw'])}
        protected = [accepted['baseline']['key']]
        if packet.get('contract') == store.CONTRACT:
            publication.update(status='complete_original_sources_and_calendar_replayed', replay=store.replay(native, s3, BUCKET, packet))
            ref = packet['publication_context']['manifest']; protected.append(ref['key'])
            plan = store.strict(store.retained(s3, BUCKET, ref))
            protected.extend(a['original']['key'] for a in plan['http_attempts'] if 'original' in a)
        privacy = access.summarize([access.check(k) for k in protected])
        if not privacy['all_denied']:
            raise ValueError('Original research inputs must remain private')
        if runtime(lam, s3, events, scheduler, FN) != actual:
            raise ValueError('Runtime changed during acceptance')
        r.kv(expected_commit=expected, actual_runtime=actual, compiler_sha256=store.compiler_hashes(), native_publication=publication, **privacy,
             native_invocations=0, provider_requests=0, public_writes=0, history_writes=0, schedule_changes=0, account_reads=0,
             scope='Complete native legacy calculation plus exact-month heavy-truck and complete GPR workbook replay. One conditional public head. Not original release vintages, demand inference, forecast performance or portfolio permission.')


if __name__ == '__main__':
    try:
        main()
    except Exception:
        sys.exit(1)
