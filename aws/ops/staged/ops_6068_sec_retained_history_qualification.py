"""Qualify all twelve complete retained originals without repeating a request."""
from pathlib import Path
from copy import deepcopy
from concurrent.futures import ThreadPoolExecutor
import sys
import boto3
ROOT = Path(__file__).resolve().parents[3]
sys.path[:0] = [str(ROOT / p) for p in ('aws/ops', 'aws/ops/staged', 'aws/ops/checks')]
from ops_report import report
from market_runtime_evidence import runtime
from ops_5998_option_population_retained_acceptance import denied_with_retry
import ops_6066_sec_complete_observed_history as previous
import ops_6067_sec_all_archive_field_diagnosis as diagnosis
import sec_ftd_inventory as sec
base, raw, BUCKET, PRIVATE = previous.base, previous.raw, previous.BUCKET, previous.PRIVATE
REQUEST = 'chatgpt-sec-retained-history-qualification-6068'
STATUS = PRIVATE + 'requests/' + raw.sha(REQUEST.encode()) + '.json'


def journal(s3, value, claim=False):
    body = raw.encoded(value)
    s3.put_object(Bucket=BUCKET, Key=STATUS, Body=body, ContentType='application/json', CacheControl='no-store',
                  **({'IfNoneMatch': '*'} if claim else {}))
    assert base.read(s3, STATUS) == body


def reconstruct(s3, baseline, failed):
    assert failed['request_id'] == previous.REQUEST and failed['status'] == 'failed'
    assert {u: v['original'] for u, v in failed['captures'].items()} == diagnosis.ORIGINALS
    selected, originals, index, cutoff = previous.plan(s3, baseline)
    assert set(failed['captures']) == set(selected) and len(selected) == 12
    for url, row in originals.items():
        assert failed['captures'][url]['original'] == row['original']
    retained = {}
    for url in selected:
        row = deepcopy(failed['captures'][url])
        assert row['url'] == url and row['http_status'] == 200 and row['status'] == 'response_retained'
        inventory = sec.inventory(base.checked(s3, row['original']), url, row['received_at'][:10])
        if 'inventory' in row:
            assert all(inventory[k] == v for k, v in row['inventory'].items()), 'Previously accepted inventory changed'
            row['predecessor_inventory'] = row['inventory']
        if 'source_validation' in row:
            row['predecessor_source_validation'] = row['source_validation']
        row.update(inventory=inventory, source_validation={'status': 'passed', 'qualification_request': REQUEST})
        retained[url] = row
    cross = previous.cross_archive_inventory(s3, retained, selected)
    return selected, retained, index, cutoff, cross


def main():
    s3, lam = boto3.client('s3', region_name='us-east-1'), boto3.client('lambda', region_name='us-east-1')
    scheduler, events = boto3.client('scheduler', region_name='us-east-1'), boto3.client('events', region_name='us-east-1')
    with report('ops_6068_sec_retained_history_qualification') as r:
        try:
            state = raw.strict(base.read(s3, STATUS))
        except Exception as exc:
            if not raw.missing(exc):
                raise
            state = None
        if state:
            assert state['status'] == 'complete', 'Never repeat failed/ambiguous source qualification'
            ref = state['manifest']
            manifest = raw.strict(base.checked(s3, ref))
        else:
            accepted = raw.strict(base.read(s3, base.STATUS))
            assert accepted['status'] == 'complete'
            baseline_ref = accepted['manifest']
            baseline = raw.strict(base.checked(s3, baseline_ref))
            failed_bytes = base.read(s3, previous.key('campaign'))
            failed = raw.strict(failed_bytes)
            assert failed['baseline'] == baseline_ref
            diagnostic = (ROOT / 'aws/ops/reports/latest/ops_6067_sec_all_archive_field_diagnosis.md').read_bytes()
            assert b'**Status:** success' in diagnostic
            before = runtime(lam, s3, events, scheduler, base.FUNCTION)
            assert before == baseline['runtime']
            arn = lam.get_function_configuration(FunctionName=base.FUNCTION)['FunctionArn']
            bindings = base.bindings(scheduler, events, arn)
            journal(s3, {'request_id': REQUEST, 'status': 'claimed', 'baseline': baseline_ref}, True)
            try:
                selected, retained, index, cutoff, cross = reconstruct(s3, baseline, failed)
                assert runtime(lam, s3, events, scheduler, base.FUNCTION) == before
                after = base.bindings(scheduler, events, arn)
                for field in ('schedules', 'classic_default_bus_rules'):
                    assert after[field] == bindings[field] == baseline['bindings'][field]
                manifest = {'contract': 'sec-settlement-complete-sources.v1', 'request_id': REQUEST, 'generated_at': raw.now(),
                    'baseline': baseline_ref, 'baseline_inventory_parser': baseline['inventory_parser'],
                    'inventory_parser': base.retain(s3, (ROOT / 'aws/ops/checks/sec_ftd_inventory.py').read_bytes()),
                    'selection_cutoff': cutoff, 'selected_archives': selected, 'index': index, 'captures': retained,
                    'adopted_failed_journals': {'complete_campaign': base.retain(s3, failed_bytes)},
                    'source_diagnostic_report': base.retain(s3, diagnostic), 'cross_archive_inventory': cross,
                    'runtime': before, 'bindings': bindings, 'source_originals_retained': True,
                    'historical_availability_verified': False, 'snapshot_atomic': False,
                    'security_continuity_verified': False, 'forecast_qualified': False, 'sizing_qualified': False}
                ref = base.retain(s3, raw.encoded(manifest))
                journal(s3, {'request_id': REQUEST, 'status': 'complete', 'manifest': ref})
            except Exception as exc:
                journal(s3, {'request_id': REQUEST, 'status': 'failed', 'error_type': type(exc).__name__, 'baseline': baseline_ref})
                raise
        protected = {STATUS, ref['key'], manifest['baseline']['key'], manifest['inventory_parser']['key'],
            manifest['baseline_inventory_parser']['key'], manifest['source_diagnostic_report']['key'],
            manifest['index']['original']['key'], *(v['original']['key'] for v in manifest['captures'].values()),
            *(v['key'] for v in manifest['adopted_failed_journals'].values())}
        def deny(key):
            assert denied_with_retry('https://justhodl.ai/' + key)
            assert denied_with_retry('https://' + BUCKET + '.s3.amazonaws.com/' + key)
        with ThreadPoolExecutor(max_workers=3) as pool:
            for _ in pool.map(deny, sorted(protected)):
                pass
        r.kv(manifest=ref, inventories={u: v['inventory'] for u, v in manifest['captures'].items()},
             original_bytes=sum(v['original']['bytes'] for v in manifest['captures'].values()),
             original_rows=sum(v['inventory']['rows'] for v in manifest['captures'].values()),
             cross_archive_inventory=manifest['cross_archive_inventory'], protected_artifacts_checked=len(protected),
             reused_archives=12, provider_requests=0, engine_invocations=0, public_head_writes=0,
             private_account_reads=0, paid_ai_calls=0, notifications_sent=0, portfolio_writes=0, schedules_changed=0)


if __name__ == '__main__':
    try:
        main()
    except Exception:
        sys.exit(1)
