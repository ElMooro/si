"""Promote the accepted complete population to private, replayed readiness.

Does not publish a current head, invoke a producer or consumer, acquire vendor
data, modify a native package/schedule or use a private account or paid AI.
The real native runtime still requires its normal scheduled publication proof.
"""
from pathlib import Path
import json, resource, subprocess, sys, time
import boto3
from botocore.config import Config
ROOT = Path(__file__).resolve().parents[3]
sys.path[:0] = [str(ROOT / p) for p in ('aws/shared', 'aws/ops', 'aws/ops/staged', 'aws/ops/checks')]
from ops_report import report
from market_runtime_evidence import runtime
import share_structure_campaign as campaign
import ops_6090_capital_structure_research_candidate as candidate
import capital_structure_readiness as readiness
import capital_structure_producer as producer
import capital_structure_store as store
import capital_structure_source as source

BUCKET = 'justhodl-dashboard-live'
FUNCTION = 'justhodl-share-flows'
REQUEST = 'chatgpt-capital-structure-private-readiness-6123'


def accepted_population(value):
    if (not isinstance(value, dict) or value.get('status') != 'complete'
            or value.get('request_id') != candidate.REQUEST
            or value.get('counts', {}).get('reported_names') != 1615
            or value.get('counts', {}).get('provider_responses') != 11305
            or value.get('counts', {}).get('provider_rows') != 59256
            or value.get('qualification', {}).get('all_original_rows_conserved') is not True
            or value.get('qualification', {}).get('production_measurement_formulas_imported') is not False
            or value.get('qualification', {}).get('forecast_qualified') is not False
            or value.get('qualification', {}).get('sizing_qualified') is not False
            or value.get('public_artifacts_checked') != 1626):
        raise ValueError('Accepted complete independent population required')
    return value


def main():
    for path in ('tests/test_capital_structure_private_readiness.py', 'tests/test_capital_structure_readiness.py'):
        subprocess.run([sys.executable, str(ROOT / path)], cwd=ROOT, check=True)
    s3 = boto3.client('s3', region_name='us-east-1', config=Config(max_pool_connections=12, retries={'max_attempts':2}))
    lam, events, scheduler = (boto3.client(name, region_name='us-east-1') for name in ('lambda','events','scheduler'))
    with report('ops_6123_capital_structure_private_readiness') as r:
        accepted = accepted_population(campaign.read_journal(s3, candidate.STATUS))
        before = runtime(lam, s3, events, scheduler, FUNCTION)
        old = producer.raw(s3, BUCKET, producer.CURRENT)
        read = store.reader(s3, BUCKET)
        recorded = store.verified_run(accepted['replay'], read)
        packet = store.checked(recorded['output'], 'outputs', read)
        clocks = {key: packet[key] for key in ('generated_at','source_acquisition_started_at','source_acquisition_completed_at')}
        clocks['sec_index_requested_at'] = packet['identity_index']['requested_at']
        del packet, read
        started = time.monotonic()
        result = readiness.run(s3, BUCKET, REQUEST, accepted['source_manifest'], accepted['identity_capture'], remaining_seconds=3000)
        if result['replay'] != accepted['replay']:
            raise ValueError('Qualified original-source replay changed from accepted candidate')
        ready = source.strict(producer.raw(s3, BUCKET, producer.READY))
        if ready['replay'] != accepted['replay'] or ready['status'] != 'qualified':
            raise ValueError('Private readiness readback differs')
        if producer.raw(s3, BUCKET, producer.CURRENT) != old or runtime(lam, s3, events, scheduler, FUNCTION) != before:
            raise ValueError('Native state changed during private qualification; review before deployment')
        r.kv(request_id=REQUEST, result=result, counts=ready['counts'], qualification=ready['qualification'],
            qualified_at=ready['qualified_at'], original_clocks=clocks, ready_sha256=source.sha(source.encoded(ready)),
            elapsed_seconds=round(time.monotonic()-started,3), max_rss_kib=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
            native_package_unchanged=True, native_runtime_capacity_verified=False,
            provider_requests=0, producer_invocations=0, consumer_invocations=0, current_head_writes=0,
            private_account_reads=0, notifications_sent=0, paid_ai_calls=0, schedules_changed=0,
            original_sec_filings_verified=False, historical_security_continuity_verified=False,
            forecast_qualified=False, sizing_qualified=False)


if __name__ == '__main__':
    try: main()
    except Exception: sys.exit(1)
