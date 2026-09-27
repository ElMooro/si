"""Read-only exact package, unchanged scheduling and normal-source calendar replay."""
from pathlib import Path
import json, subprocess, sys
import boto3
ROOT = Path(__file__).resolve().parents[3]
sys.path[:0] = [str(ROOT/p) for p in ('aws/ops', 'aws/ops/checks', 'aws/lambdas/justhodl-port-cargo/source', 'aws/shared')]
from ops_report import report
from market_runtime_evidence import runtime
import cargo_store as store
import cargo_measurements as measurements
import lambda_function as native
import retained_access_evidence as access
FN = 'justhodl-port-cargo'; BUCKET = 'justhodl-dashboard-live'


def main():
    lam, s3, events, scheduler = (boto3.client(name, region_name='us-east-1') for name in ('lambda', 's3', 'events', 'scheduler'))
    with report('ops_6212_port_cargo_calendar_acceptance') as r:
        expected = subprocess.check_output(['git', 'log', '-1', '--format=%H', '--', 'aws/lambdas/'+FN], cwd=ROOT, text=True).strip()
        before = runtime(lam, s3, events, scheduler, FN)
        r.kv(actual_runtime_before_validation=before)
        if before['receipt'] != {'status': 'matched', 'commit': expected} or before['source_files_checked'] != 4:
            raise ValueError('Exact four-source intended release required')
        accepted = json.loads((ROOT/'docs/audit/2026-09-27/port-cargo-source-code-schedule-acceptance.json').read_bytes())
        baseline = accepted['actual_runtime']
        for key, value in baseline.items():
            if key not in ('code_sha256', 'source_files_checked', 'handler_bytes', 'receipt') and before.get(key) != value:
                raise ValueError('Original runtime or repaired cadence differs: '+key)
        subprocess.run([sys.executable, str(ROOT/'aws/lambdas'/FN/'tests/run_tests.py')], cwd=ROOT, check=True)
        obj = s3.get_object(Bucket=BUCKET, Key=store.HEAD)
        raw = store.whole(obj['Body'], obj.get('ContentLength')); packet = store.decode(raw)
        publication = {'status': 'pending_original_daily_1240_publication', 'generated_at': packet.get('generated_at'),
                       'version': packet.get('version'), 'whole_bytes': len(raw), 'whole_sha256': store.sha(raw)}
        if packet.get('contract') == store.CONTRACT and (packet.get('measurement_review') or {}).get('contract') == measurements.CONTRACT:
            publication.update(status='whole_native_source_membership_and_calendar_replayed', replay=store.replay(native, s3, BUCKET, packet),
                               acquisition_review=packet['acquisition_review'],
                               matched_import_ports=packet['measurement_review']['covered_port_cohort']['legs']['import']['matched_ports'],
                               matched_export_ports=packet['measurement_review']['covered_port_cohort']['legs']['export']['matched_ports'],
                               source_latest_date=packet['measurement_review']['source_latest_date'])
        protected = [accepted['repair']['complete_predecessors']['key']]
        if packet.get('contract') == store.CONTRACT:
            protected.append(packet['publication_context']['manifest']['key'])
        privacy = access.summarize([access.check(key) for key in protected])
        if not privacy['all_denied']: raise ValueError('Retained research originals must remain private')
        if runtime(lam, s3, events, scheduler, FN) != before: raise ValueError('Runtime changed during verification')
        r.kv(expected_commit=expected, actual_runtime=before, compiler_sha256=store.hashes(), native_publication=publication, **privacy,
             native_invocations=0, provider_requests=0, archive_writes=0, public_writes=0, schedule_changes=0, account_reads=0,
             scope='Complete source/catalog membership, exact seven-day and preceding 28-day estimates over identical covered port cohorts. This is not a world-port census, atomic provider snapshot, customs trade value, holiday-adjusted signal, forecast or portfolio qualification.')


if __name__ == '__main__':
    try: main()
    except Exception: sys.exit(1)
