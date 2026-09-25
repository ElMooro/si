"""Retain six advertised months of SEC CNS balances; no native publication.

Reuse the two complete 6060 archives. Every other public archive request is
durably claimed exactly once. Failed/ambiguous campaigns are never recollected.
"""
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
import sys, time
import boto3
ROOT = Path(__file__).resolve().parents[3]
sys.path[:0] = [str(ROOT / p) for p in ('aws/ops', 'aws/ops/staged', 'aws/ops/checks')]
from ops_report import report
from market_runtime_evidence import runtime
from ops_5998_option_population_retained_acceptance import denied_with_retry
import ops_6060_sec_control_trailer_baseline as base
import sec_ftd_inventory as sec
raw, BUCKET, PRIVATE = base.raw, base.BUCKET, base.PRIVATE
REQUEST = 'chatgpt-sec-advertised-archive-history-6062'
BASELINE_PARSER_SHA = '059ac8256e4275b28108dde40fa5dacdab0167a87b9e82a5e8a23fb7d3023317'


def key(label):
    return PRIVATE + 'requests/' + raw.sha((REQUEST + ':' + label).encode()) + '.json'


def journal(s3, label, value, claim=False):
    body = raw.encoded(value)
    assert len(body) <= sec.MAX_ZIP
    s3.put_object(Bucket=BUCKET, Key=key(label), Body=body, ContentType='application/json',
                  CacheControl='no-store', **({'IfNoneMatch': '*'} if claim else {}))
    assert base.read(s3, key(label)) == body


def successful(s3, captured):
    assert captured['http_status'] == 200 and captured['status'] == 'response_retained'
    return base.checked(s3, captured['original'])


def plan(s3, baseline):
    assert baseline['contract'] == 'squeeze-settlement-source-baseline.v1'
    assert baseline['request_id'] == base.REQUEST
    assert raw.sha(base.checked(s3, baseline['inventory_parser'])) == BASELINE_PARSER_SHA
    captures = baseline['captures']
    assert set(captures) == {'0', '1', '2'} and captures['0']['url'] == sec.INDEX
    index = successful(s3, captures['0'])
    cutoff = captures['0']['received_at'][:10]
    assert baseline['selection_cutoff'] == cutoff
    selected = sec.advertised_archives(index, cutoff, 12)
    assert baseline['advertised_selected_archives'] == selected[:2]
    retained = {}
    for i, url in enumerate(selected[:2], 1):
        row = captures[str(i)]
        assert row['url'] == url
        assert sec.inventory(successful(s3, row), url, row['received_at'][:10]) == row['inventory']
        retained[url] = row
    return selected, retained, captures['0'], cutoff


def fetch(s3, url, deadline, capture=base.capture):
    sec.archive_period(url)
    label = 'source:' + url
    journal(s3, label, {'request_id': REQUEST, 'status': 'claimed', 'url': url}, True)
    try:
        if time.monotonic() >= deadline:
            raise TimeoutError('Reviewed SEC campaign deadline')
        result = capture(s3, url)
        journal(s3, label, {'request_id': REQUEST, 'status': 'response_retained', 'capture': result})
        body = successful(s3, result)
        result['inventory'] = sec.inventory(body, url, result['received_at'][:10])
        journal(s3, label, {'request_id': REQUEST, 'status': 'complete', 'capture': result})
        return result
    except Exception as exc:
        journal(s3, label, {'request_id': REQUEST, 'status': 'failed', 'url': url, 'error_type': type(exc).__name__,
                            **({'capture': result} if 'result' in locals() else {})})
        raise


def main():
    s3, lam = boto3.client('s3', region_name='us-east-1'), boto3.client('lambda', region_name='us-east-1')
    scheduler, events = boto3.client('scheduler', region_name='us-east-1'), boto3.client('events', region_name='us-east-1')
    with report('ops_6062_sec_advertised_archive_history') as r:
        try:
            prior = raw.strict(base.read(s3, key('campaign')))
        except Exception as exc:
            if not raw.missing(exc):
                raise
            prior = None
        if prior:
            assert prior['status'] == 'complete', 'Never repeat failed/ambiguous archive qualification'
            ref = prior['manifest']
            manifest = raw.strict(base.checked(s3, ref))
            provider_requests = 0
        else:
            accepted = raw.strict(base.read(s3, base.STATUS))
            assert accepted['status'] == 'complete', 'Source baseline must be accepted first'
            baseline_ref = accepted['manifest']
            baseline = raw.strict(base.checked(s3, baseline_ref))
            selected, retained, index, cutoff = plan(s3, baseline)
            journal(s3, 'campaign', {'request_id': REQUEST, 'status': 'claimed', 'baseline': baseline_ref}, True)
            progress, provider_requests = {'captures': retained}, 0
            try:
                before = runtime(lam, s3, events, scheduler, base.FUNCTION)
                assert before == baseline['runtime'], 'Reviewed native predecessor runtime changed'
                arn = lam.get_function_configuration(FunctionName=base.FUNCTION)['FunctionArn']
                bindings = base.bindings(scheduler, events, arn)
                deadline = time.monotonic() + 240
                for url in selected:
                    if url in retained:
                        continue
                    journal(s3, 'campaign', {'request_id': REQUEST, 'status': 'capturing', 'baseline': baseline_ref,
                                            'next_source': url, **progress})
                    provider_requests += 1
                    retained[url] = fetch(s3, url, deadline)
                    assert sum(v['original']['bytes'] for v in retained.values()) <= 96 * 1024 * 1024
                    journal(s3, 'campaign', {'request_id': REQUEST, 'status': 'capturing', 'baseline': baseline_ref, **progress})
                    time.sleep(0.2)
                assert set(retained) == set(selected) and provider_requests == 10
                assert runtime(lam, s3, events, scheduler, base.FUNCTION) == before
                after = base.bindings(scheduler, events, arn)
                for field in ('schedules', 'classic_default_bus_rules'):
                    assert after[field] == bindings[field] == baseline['bindings'][field]
                manifest = {'contract': 'sec-settlement-complete-sources.v1', 'request_id': REQUEST, 'generated_at': raw.now(),
                            'baseline': baseline_ref, 'baseline_inventory_parser': baseline['inventory_parser'],
                            'inventory_parser': base.retain(s3, (ROOT / 'aws/ops/checks/sec_ftd_inventory.py').read_bytes()),
                            'selection_cutoff': cutoff, 'selected_archives': selected, 'index': index, 'captures': retained,
                            'runtime': before, 'bindings': bindings,
                            'source_originals_retained': True, 'historical_availability_verified': False,
                            'snapshot_atomic': False, 'security_continuity_verified': False,
                            'forecast_qualified': False, 'sizing_qualified': False}
                ref = base.retain(s3, raw.encoded(manifest))
                journal(s3, 'campaign', {'request_id': REQUEST, 'status': 'complete', 'manifest': ref})
            except Exception as exc:
                journal(s3, 'campaign', {'request_id': REQUEST, 'status': 'failed', 'baseline': baseline_ref,
                                        'error_type': type(exc).__name__, **progress})
                r.kv(error_type=type(exc).__name__, provider_requests=provider_requests, retained_progress=progress,
                     engine_invocations=0, public_head_writes=0, private_account_reads=0)
                raise
        protected = {key('campaign'), manifest['baseline']['key'], ref['key'], manifest['inventory_parser']['key'],
                     manifest['baseline_inventory_parser']['key'],
                     manifest['index']['original']['key'], *(v['original']['key'] for v in manifest['captures'].values()),
                     *(key('source:' + url) for url in manifest['selected_archives'][2:])}
        def deny(path):
            assert denied_with_retry('https://justhodl.ai/' + path) and denied_with_retry('https://' + BUCKET + '.s3.amazonaws.com/' + path)
        with ThreadPoolExecutor(max_workers=3) as pool:
            for _ in pool.map(deny, sorted(protected)):
                pass
        r.kv(manifest=ref, selected_archives=manifest['selected_archives'],
             inventories={url: cap['inventory'] for url, cap in manifest['captures'].items()},
             original_bytes=sum(cap['original']['bytes'] for cap in manifest['captures'].values()),
             original_rows=sum(cap['inventory']['rows'] for cap in manifest['captures'].values()),
             protected_artifacts_checked=len(protected), provider_requests=provider_requests, reused_archives=2,
             engine_invocations=0, public_head_writes=0, private_account_reads=0, paid_ai_calls=0,
             notifications_sent=0, portfolio_writes=0, schedules_changed=0)


if __name__ == '__main__':
    try:
        main()
    except Exception:
        sys.exit(1)
