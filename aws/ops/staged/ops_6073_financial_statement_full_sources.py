"""Capture the complete retained 500-name universe with bounded FMP requests.

Reuse all twelve successful probe originals. New requests are durably claimed,
limited to 2.5 starts/second and three workers. No model/head/signal writes.
"""
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
from collections import Counter
import json, subprocess, sys
import boto3
from botocore.config import Config
ROOT = Path(__file__).resolve().parents[3]
sys.path[:0] = [str(ROOT / p) for p in ('aws/ops', 'aws/ops/staged', 'aws/ops/checks')]
from ops_report import report
from market_runtime_evidence import runtime
from ops_5998_option_population_retained_acceptance import denied_with_retry
from financial_statement_inventory import records
import financial_statement_campaign as campaign
import financial_statement_source as source
import ops_6071_financial_statement_source_inventory as baseline
import ops_6072_financial_statement_original_probe as probe
BUCKET = baseline.BUCKET
REQUEST = 'chatgpt-financial-statements-full-6073'
STATUS = campaign.request_key(REQUEST, 'campaign')
PROBE = {'key': baseline.PRIVATE + '15b643c84dac5dea7dd4853673fcf3f8f7440ded6e9dafb72e97abd5bd39f13d.bin',
    'sha256': '15b643c84dac5dea7dd4853673fcf3f8f7440ded6e9dafb72e97abd5bd39f13d', 'bytes': 50234}


def main():
    s3 = boto3.client('s3', region_name='us-east-1', config=Config(max_pool_connections=12, retries={'max_attempts': 2}))
    lam, events, scheduler = (boto3.client(name, region_name='us-east-1') for name in ('lambda', 'events', 'scheduler'))
    with report('ops_6073_financial_statement_full_sources') as r:
        subprocess.run([sys.executable, str(ROOT / 'tests/test_financial_statement_campaign.py')], cwd=ROOT, check=True)
        original = json.loads(campaign.read(s3, probe.BASELINE))
        diagnostic = json.loads(campaign.read(s3, PROBE))
        universe_ref = original['captures']['screener/data.json']['original']
        population = records(json.loads(campaign.read(s3, universe_ref)), 'universe')
        labels = [row.get('symbol') or row.get('ticker') for row in population]
        assert len(labels) == 500 and len(set(labels)) == 500
        symbols = frozenset(labels)
        specs = [source.request_spec(symbol, endpoint, period, symbols) for symbol in sorted(symbols)
            for period in ('annual', 'quarter') for endpoint in source.ENDPOINTS]
        assert len(specs) == 3000
        prior = {fn: runtime(lam, s3, events, scheduler, fn) for fn in baseline.FUNCTIONS}
        assert prior == original['runtime']
        adopted = {}
        for value in diagnostic['captures'].values():
            spec = value['spec']; assert spec in specs and value['http_status'] == 200
            body = campaign.read(s3, value['original'])
            inv = campaign.inventory(body, spec, symbols)
            assert {k: v for k, v in inv.items() if k != 'status'} == value['inventory']
            capture = {**value, 'inventory': inv, 'adopted_from_manifest': PROBE, 'reused_original': True}
            adopted[spec['url']] = {**capture, 'retained_capture': campaign.retain(s3, campaign.encode(capture))}
        assert len(adopted) == 12
        cfg = lam.get_function_configuration(FunctionName='justhodl-forensic-screen')
        credential = cfg.get('Environment', {}).get('Variables', {}).get('FMP_KEY')
        if not credential:
            credential = boto3.client('ssm', region_name='us-east-1').get_parameter(Name='/justhodl/fmp/api-key', WithDecryption=True)['Parameter']['Value']
        assert isinstance(credential, str) and credential
        base = {'contract': 'financial-statement-complete-source-campaign.v1', 'request_id': REQUEST,
            'generated_at': baseline.now(), 'universe': universe_ref, 'baseline': probe.BASELINE, 'probe': PROBE,
            'reported_symbols': sorted(symbols), 'planned_sources': 3000, 'adopted_sources': 12,
            'max_new_provider_requests': 2988, 'max_request_starts_per_second': 2.5, 'workers': 3,
            'snapshot_atomic': False, 'index_membership_verified': False, 'historical_availability_verified': False,
            'accounting_calculations_qualified': False, 'forecast_qualified': False, 'sizing_qualified': False}
        campaign.journal(s3, STATUS, {**base, 'status': 'claimed'}, True)
        def save(complete, errors):
            campaign.journal(s3, STATUS, {**base, 'status': 'failed' if errors else 'capturing',
                'captures': {key: value['retained_capture'] for key, value in complete.items()},
                'completed_sources': len(complete), 'source_errors': errors})
            print(json.dumps({'campaign': REQUEST, 'completed': len(complete), 'planned': 3000, 'source_errors': len(errors)}), flush=True)
        rate = campaign.Rate()
        captures = campaign.collect(specs, adopted,
            lambda spec: campaign.capture(s3, REQUEST, spec, symbols, credential, rate, baseline.now), save)
        assert len(captures) == 3000
        counts = {'complete_sources': len(captures), 'reused_sources': 12, 'new_provider_requests': 2988,
            'provider_rows': sum(value['inventory']['rows'] for value in captures.values()),
            'provider_bytes': sum(value['original']['bytes'] for value in captures.values()),
            'unavailable_empty_responses': sum(value['inventory']['rows'] == 0 for value in captures.values()),
            'responses_with_identity_problems': sum(bool(value['inventory']['identity_problems']) for value in captures.values()),
            'repeated_statement_identities': sum(value['inventory']['repeated_statement_identities'] for value in captures.values()),
            'source_statuses': dict(Counter(value['inventory']['status'] for value in captures.values()))}
        manifest = campaign.retain(s3, campaign.encode({**base, 'status': 'complete', 'completed_at': baseline.now(),
            'captures': {key: value['retained_capture'] for key, value in captures.items()}, 'counts': counts}))
        # Re-read every complete response and inventory from its immutable
        # capsule before claiming that the full source population was retained.
        for value in captures.values():
            capsule = json.loads(campaign.read(s3, value['retained_capture']))
            body = campaign.read(s3, capsule['original'])
            assert campaign.inventory(body, capsule['spec'], symbols) == capsule['inventory']
        protected = {STATUS, manifest['key'], probe.BASELINE['key'], PROBE['key'], universe_ref['key'],
            *(value['original']['key'] for value in captures.values()), *(value['retained_capture']['key'] for value in captures.values()),
            *(value['request_status_key'] for value in captures.values() if value.get('request_status_key'))}
        def deny(key):
            assert denied_with_retry('https://justhodl.ai/' + key)
            assert denied_with_retry('https://' + BUCKET + '.s3.amazonaws.com/' + key)
        with ThreadPoolExecutor(max_workers=6) as pool:
            for _ in pool.map(deny, sorted(protected)):
                pass
        assert {fn: runtime(lam, s3, events, scheduler, fn) for fn in baseline.FUNCTIONS} == prior
        campaign.journal(s3, STATUS, {**base, 'status': 'complete', 'manifest': manifest,
            'counts': counts, 'protected_artifacts_checked': len(protected)})
        r.kv(manifest=manifest, counts=counts, protected_artifacts_checked=len(protected),
            all_original_responses_reparsed=True, provider_requests=2988, producer_invocations=0, consumer_invocations=0,
            private_account_reads=0, signal_writes=0, public_writes=0, paid_ai_calls=0, notifications_sent=0,
            schedules_changed=0, accounting_calculations_qualified=False, forecast_qualified=False, sizing_qualified=False)


if __name__ == '__main__':
    try:
        main()
    except Exception:
        sys.exit(1)
