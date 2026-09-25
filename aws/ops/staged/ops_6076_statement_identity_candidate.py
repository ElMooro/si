"""Qualify the complete captured accounting universe before any native change.

No provider request, Lambda invocation, account read, current-head replacement,
signal, notification, schedule modification or paid AI. Exact original source
replay plus separately implemented integer/Fraction arithmetic for every row.
"""
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
import gc, json, resource, subprocess, sys, time, urllib.request
import boto3
from botocore.config import Config
ROOT = Path(__file__).resolve().parents[3]
sys.path[:0] = [str(ROOT / p) for p in ('aws/shared', 'aws/ops', 'aws/ops/staged', 'aws/ops/checks')]
from ops_report import report
from market_runtime_evidence import runtime
from ops_5998_option_population_retained_acceptance import denied_with_retry
import ops_6071_financial_statement_source_inventory as baseline
import ops_6072_financial_statement_original_probe as probe
import financial_statement_campaign as campaign
import statement_research_source as source
import statement_research_v2 as model
import statement_research_store_v2 as store
import statement_research_arithmetic_v2 as arithmetic
BUCKET = baseline.BUCKET
REQUEST = 'chatgpt-statement-research-identity-candidate-6076'
STATUS = campaign.request_key(REQUEST, 'candidate')
SOURCE = {'key': model.PRIVATE + '733ffdf2eefa5aa5072c81782375232c0520819528c217355e288e75c76b563c.bin',
    'sha256': '733ffdf2eefa5aa5072c81782375232c0520819528c217355e288e75c76b563c', 'bytes': 985798}

IDENTITY_DIAGNOSTIC = {'key': 'audit-private/20260909-originals/financial-statement-research/dda3e75ef15b62310d4550500047468cb7e8d97db49675382bf142dddf420395.bin', 'sha256': 'dda3e75ef15b62310d4550500047468cb7e8d97db49675382bf142dddf420395', 'bytes': 13376248}


def fingerprint(compiled):
    return {'packet': source.sha(source.encoded(compiled['packet'])),
        'shards': {key: source.sha(source.encoded(value)) for key, value in compiled['shards'].items()}}


def main():
    s3 = boto3.client('s3', region_name='us-east-1', config=Config(max_pool_connections=12, retries={'max_attempts': 2}))
    lam, events, scheduler = (boto3.client(name, region_name='us-east-1') for name in ('lambda', 'events', 'scheduler'))
    with report('ops_6076_statement_identity_candidate') as r:
        for path in ('test_statement_measurements.py', 'test_statement_research_model.py', 'test_statement_research_store.py',
                     'test_statement_research_v2.py', 'test_statement_research_store_v2.py'):
            subprocess.run([sys.executable, str(ROOT / 'tests' / path)], cwd=ROOT, check=True)
        read = store.reader(s3, BUCKET)
        accepted = json.loads(source.original(probe.BASELINE, read))
        prior = {fn: runtime(lam, s3, events, scheduler, fn) for fn in baseline.FUNCTIONS}
        assert prior == accepted['runtime']
        commit = subprocess.check_output(['git', 'log', '-1', '--format=%H', '--', 'aws/ops/staged/ops_6076_statement_identity_candidate.py'], cwd=ROOT, text=True).strip()
        progress = {'request_id': REQUEST, 'source_commit': commit, 'status': 'claimed', 'source_manifest': SOURCE}
        campaign.journal(s3, STATUS, progress, True)
        try:
            diagnostic = source.strict(source.original(IDENTITY_DIAGNOSTIC, read))
            assert diagnostic['source_manifest'] == SOURCE
            identity_ref = campaign.retain(s3, source.encoded(diagnostic['sec_index_capture']))
            del diagnostic
            gc.collect()
            started = time.monotonic()
            compiled = model.compile_output(SOURCE, identity_ref, read)
            packet = compiled['packet']
            assert (packet['reported_names'], packet['provider_responses'], packet['provider_rows']) == (500, 3000, 20941)
            profile = {'compile_seconds': round(time.monotonic() - started, 3),
                'index_bytes': len(source.encoded(packet)), 'shard_count': len(compiled['shards']),
                'shard_bytes': sum(len(source.encoded(v)) for v in compiled['shards'].values()),
                'max_shard_bytes': max(len(source.encoded(v)) for v in compiled['shards'].values())}
            progress.update(status='compiled', profile=profile)
            campaign.journal(s3, STATUS, progress)
            started = time.monotonic()
            proof = arithmetic.verify(SOURCE, identity_ref, compiled, read)
            profile['independent_check_seconds'] = round(time.monotonic() - started, 3)
            assert resource.getrusage(resource.RUSAGE_SELF).ru_maxrss < 850 * 1024, 'Candidate exceeds conservative 1024 MB native budget'
            expected = fingerprint(compiled)
            ref = store.retain(s3, BUCKET, SOURCE, identity_ref, compiled)
            progress.update(status='retained', replay=ref, qualification=proof)
            campaign.journal(s3, STATUS, progress)
            del compiled, packet, read
            gc.collect()
            started = time.monotonic()
            read = store.reader(s3, BUCKET)
            compiled = store.replay(ref, read)
            profile.update(replay_seconds=round(time.monotonic() - started, 3), max_rss_kib=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)
            assert fingerprint(compiled) == expected
            assert profile['max_rss_kib'] < 850 * 1024, 'Replay exceeds conservative 1024 MB native budget'
            assert profile['replay_seconds'] < 650, 'Source replay exceeds conservative 840-second native budget'
            run = store.verified_run(ref, read)
            public = {ref['manifest_key'], run['input']['key'], run['output']['key'],
                compiled['packet']['identity_index']['original']['key'], *(v['key'] for v in run['compilers'].values()), *(v['record']['key'] for v in compiled['packet']['issuers'])}
            def check_public(key):
                request = urllib.request.Request('https://justhodl.ai/' + key,
                    headers={'User-Agent': 'JustHodl-research-acceptance/1.0', 'Cache-Control': 'no-cache'})
                assert store.bounded(urllib.request.urlopen(request, timeout=30)) == store.bounded(s3.get_object(Bucket=BUCKET, Key=key)['Body'])
            with ThreadPoolExecutor(max_workers=4) as pool:
                for _ in pool.map(check_public, sorted(public)):
                    pass
            # The complete 8,993 original paths were denied in ops 6073.
            # This operation creates one new protected journal and an immutable identity capture capsule.
            for key in (STATUS, SOURCE['key'], IDENTITY_DIAGNOSTIC['key'], identity_ref['key']):
                assert denied_with_retry('https://justhodl.ai/' + key)
                assert denied_with_retry('https://' + BUCKET + '.s3.amazonaws.com/' + key)
            assert {fn: runtime(lam, s3, events, scheduler, fn) for fn in baseline.FUNCTIONS} == prior
            packet = compiled['packet']
            counts = {key: packet[key] for key in ('reported_names', 'provider_responses', 'provider_rows', 'records',
                'complete_aligned_records', 'records_with_repeated_endpoints', 'rows_with_identity_problems',
                'record_statuses', 'metric_statuses', 'multiple_labels_per_issuer', 'clock_issues',
                'current_identity_statuses', 'current_identity_quarantined_rows', 'records_without_calculations')}
            progress.update(status='complete', profile=profile, qualification=proof, counts=counts,
                identity_capture=identity_ref, public_artifacts_checked=len(public), frozen_compilers={key: value['sha256'] for key, value in run['compilers'].items()})
            campaign.journal(s3, STATUS, progress)
            r.kv(**progress, native_packages_unchanged=True, provider_requests=0, engine_invocations=0,
                native_head_writes=0, private_account_reads=0, paid_ai_calls=0, notifications_sent=0,
                signal_writes=0, portfolio_writes=0, schedules_changed=0, original_sec_filings_verified=False,
                historical_availability_verified=False, forecast_qualified=False, sizing_qualified=False)
        except Exception as exc:
            campaign.journal(s3, STATUS, {**progress, 'status': 'failed', 'error_type': type(exc).__name__})
            raise


if __name__ == '__main__':
    try:
        main()
    except Exception:
        sys.exit(1)
