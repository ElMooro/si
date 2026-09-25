"""Retain six advertised months using actual source settlement dates.

Reuse both 6060 archives and the complete failed July capture. Every new request is
durably claimed exactly once. Failed/ambiguous campaigns are never recollected.
"""
from pathlib import Path
from collections import defaultdict
from copy import deepcopy
from concurrent.futures import ThreadPoolExecutor
import sys, time, zipfile
import boto3
ROOT = Path(__file__).resolve().parents[3]
sys.path[:0] = [str(ROOT / p) for p in ('aws/ops', 'aws/ops/staged', 'aws/ops/checks')]
from ops_report import report
from market_runtime_evidence import runtime
from ops_5998_option_population_retained_acceptance import denied_with_retry
import ops_6060_sec_control_trailer_baseline as base
import ops_6062_sec_advertised_archive_history as predecessor
import sec_ftd_inventory as sec
raw, BUCKET, PRIVATE = base.raw, base.BUCKET, base.PRIVATE
REQUEST = 'chatgpt-sec-reported-settlement-history-6064'
BASELINE_PARSER_SHA = '059ac8256e4275b28108dde40fa5dacdab0167a87b9e82a5e8a23fb7d3023317'
JULY = 'https://www.sec.gov/files/data/fails-deliver-data/cnsfails202607b.zip'
JULY_ORIGINAL = {'key': PRIVATE + 'dc7d52c6eaf5c3cd18cbd8f6700ed1c8f9311a2a5d2c710b74f2a697384fbdea.bin',
                 'sha256': 'dc7d52c6eaf5c3cd18cbd8f6700ed1c8f9311a2a5d2c710b74f2a697384fbdea', 'bytes': 1653974}


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
        row = deepcopy(captures[str(i)])
        assert row['url'] == url
        reconstructed = sec.inventory(successful(s3, row), url, row['received_at'][:10])
        assert all(reconstructed[k] == v for k, v in row['inventory'].items())
        row['predecessor_inventory'] = row['inventory']
        row['inventory'] = reconstructed
        retained[url] = row
    return selected, retained, captures['0'], cutoff


def adopt_july(s3, baseline_ref, selected, retained):
    assert selected[2] == JULY and set(retained) == set(selected[:2])
    old_state = base.read(s3, predecessor.key('campaign'))
    old_request = base.read(s3, predecessor.key('source:' + JULY))
    failed, request = raw.strict(old_state), raw.strict(old_request)
    assert failed['status'] == request['status'] == 'failed'
    assert failed['request_id'] == request['request_id'] == predecessor.REQUEST
    assert failed['baseline'] == baseline_ref
    assert set(failed['captures']) == set(retained)
    for url in retained:
        assert failed['captures'][url]['original'] == retained[url]['original']
    captured = deepcopy(request['capture'])
    assert captured['url'] == JULY and captured['original'] == JULY_ORIGINAL
    captured['inventory'] = sec.inventory(successful(s3, captured), JULY, captured['received_at'][:10])
    retained[JULY] = captured
    return {'campaign': base.retain(s3, old_state), 'request': base.retain(s3, old_request)}


def cross_archive_inventory(s3, captured, selected):
    seen, dates, identical, conflicting, examples = {}, defaultdict(list), 0, 0, []
    for url in selected:
        member, body = sec.text_member(successful(s3, captured[url]))
        records = sec.rows(body, url, captured[url]['received_at'][:10])
        assert member == captured[url]['inventory']['member']
        for stamp in sorted({row['settlement_date'] for row in records}):
            dates[stamp].append(url)
        for row in records:
            key = (row['settlement_date'], row['cusip'])
            value = (row['symbol'], row['description'], row['fail_balance_shares'], row['reported_price'])
            if key in seen:
                if value == seen[key]:
                    identical += 1
                else:
                    conflicting += 1
                    if len(examples) < 8:
                        examples.append({'settlement_date': key[0], 'cusip': key[1], 'first_reported_fields': list(seen[key]),
                                         'other_reported_fields': list(value), 'other_archive': url, 'other_source_line': row['source_line']})
            else:
                seen[key] = value
    return {'unique_reported_date_cusips': len(seen), 'identical_cross_archive_repetitions': identical,
            'conflicting_cross_archive_records': conflicting, 'conflict_examples': examples,
            'overlapping_dates': {stamp: urls for stamp, urls in sorted(dates.items()) if len(urls) > 1},
            'reported_dates': sorted(dates), 'source_rows_discarded': False, 'conflicting_records_silently_overwritten': False,
            'security_continuity_verified': False, 'full_trading_calendar_coverage_verified': False}


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
        try:
            result['inventory'] = sec.inventory(body, url, result['received_at'][:10])
        except (ValueError, zipfile.BadZipFile) as exc:
            result['source_validation'] = {'status': 'failed', 'error_type': type(exc).__name__, 'reason': str(exc)[:400]}
            journal(s3, label, {'request_id': REQUEST, 'status': 'validation_failed', 'capture': result})
            return result
        result['source_validation'] = {'status': 'passed'}
        journal(s3, label, {'request_id': REQUEST, 'status': 'complete', 'capture': result})
        return result
    except Exception as exc:
        journal(s3, label, {'request_id': REQUEST, 'status': 'failed', 'url': url, 'error_type': type(exc).__name__,
                            **({'capture': result} if 'result' in locals() else {})})
        raise


def main():
    s3, lam = boto3.client('s3', region_name='us-east-1'), boto3.client('lambda', region_name='us-east-1')
    scheduler, events = boto3.client('scheduler', region_name='us-east-1'), boto3.client('events', region_name='us-east-1')
    with report('ops_6064_sec_reported_settlement_history') as r:
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
            adopted_journals = adopt_july(s3, baseline_ref, selected, retained)
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
                assert set(retained) == set(selected) and provider_requests == 9
                invalid = {url: cap.get('source_validation') for url, cap in retained.items() if 'inventory' not in cap}
                assert not invalid, 'Complete originals retained; source validation errors: ' + str(invalid)
                cross_archive = cross_archive_inventory(s3, retained, selected)
                assert runtime(lam, s3, events, scheduler, base.FUNCTION) == before
                after = base.bindings(scheduler, events, arn)
                for field in ('schedules', 'classic_default_bus_rules'):
                    assert after[field] == bindings[field] == baseline['bindings'][field]
                manifest = {'contract': 'sec-settlement-complete-sources.v1', 'request_id': REQUEST, 'generated_at': raw.now(),
                            'baseline': baseline_ref, 'baseline_inventory_parser': baseline['inventory_parser'],
                            'inventory_parser': base.retain(s3, (ROOT / 'aws/ops/checks/sec_ftd_inventory.py').read_bytes()),
                            'selection_cutoff': cutoff, 'selected_archives': selected, 'index': index, 'captures': retained,
                            'adopted_failed_journals': adopted_journals, 'cross_archive_inventory': cross_archive,
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
                     *(v['key'] for v in manifest['adopted_failed_journals'].values()),
                     *(key('source:' + url) for url in manifest['selected_archives'][3:])}
        def deny(path):
            assert denied_with_retry('https://justhodl.ai/' + path) and denied_with_retry('https://' + BUCKET + '.s3.amazonaws.com/' + path)
        with ThreadPoolExecutor(max_workers=3) as pool:
            for _ in pool.map(deny, sorted(protected)):
                pass
        r.kv(manifest=ref, selected_archives=manifest['selected_archives'],
             inventories={url: cap['inventory'] for url, cap in manifest['captures'].items()},
             original_bytes=sum(cap['original']['bytes'] for cap in manifest['captures'].values()),
             original_rows=sum(cap['inventory']['rows'] for cap in manifest['captures'].values()),
             cross_archive_inventory=manifest['cross_archive_inventory'],
             protected_artifacts_checked=len(protected), provider_requests=provider_requests, reused_archives=3,
             engine_invocations=0, public_head_writes=0, private_account_reads=0, paid_ai_calls=0,
             notifications_sent=0, portfolio_writes=0, schedules_changed=0)


if __name__ == '__main__':
    try:
        main()
    except Exception:
        sys.exit(1)
