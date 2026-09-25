"""Adopt the complete failed 6057 capture with reconciled SEC file controls.

6057 is never rerun. Its index and latest ZIP are reused byte-for-byte. Only the
uncaptured first-half archive may be requested, once under this new journal.
"""
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
import copy, subprocess, sys
import boto3
ROOT = Path(__file__).resolve().parents[3]
sys.path[:0] = [str(ROOT / p) for p in ('aws/ops', 'aws/ops/staged', 'aws/ops/checks')]
from ops_report import report
from market_runtime_evidence import runtime
from ops_5998_option_population_retained_acceptance import denied_with_retry
import ops_6057_squeeze_settlement_source_baseline as base
import sec_ftd_inventory as sec
raw, BUCKET, PRIVATE, FUNCTION = base.raw, base.BUCKET, base.PRIVATE, base.FUNCTION
CURRENT = base.CURRENT
read, retain, checked, bindings = base.read, base.retain, base.checked, base.bindings
capture = base.capture
REQUEST = 'chatgpt-sec-control-trailer-baseline-6060'
STATUS = PRIVATE + 'requests/' + raw.sha(REQUEST.encode()) + '.json'
SOURCE_COMMIT = '8b1fe7d90fe645cdbe77f83569d1702ae9f2b13d'
ORIGINALS = {
    '0': ('5642ce14c42bde9a85094b47b7adce32a4e4430e35be540b49f9437e409fcdba', 418830),
    '1': ('0c90519d0d0e959b86331c5fe546cefa027c8a17998111270001b0ee61bbf203', 1393881)}


def journal(s3, value, claim=False):
    body = raw.encoded(value)
    assert len(body) <= sec.MAX_ZIP
    s3.put_object(Bucket=BUCKET, Key=STATUS, Body=body, ContentType='application/json',
                  CacheControl='no-store', **({'IfNoneMatch': '*'} if claim else {}))
    assert read(s3, STATUS) == body


def adopt(s3, failed):
    assert failed['request_id'] == base.REQUEST and failed['status'] == 'failed'
    assert failed['error_type'] == 'ValueError' and failed['commit'] == SOURCE_COMMIT
    assert set(failed['captures']) == set(ORIGINALS)
    captured = copy.deepcopy(failed['captures'])
    for label, (sha, size) in ORIGINALS.items():
        value = captured[label]
        assert value['original'] == {'key': PRIVATE + sha + '.bin', 'sha256': sha, 'bytes': size}
        assert value['http_status'] == 200 and value['status'] == 'response_retained'
        checked(s3, value['original'])
    assert captured['0']['url'] == sec.INDEX
    cutoff = captured['0']['received_at'][:10]
    selected = sec.advertised_archives(checked(s3, captured['0']['original']), cutoff, 2)
    assert captured['1']['url'] == selected[0]
    captured['1']['inventory'] = sec.inventory(checked(s3, captured['1']['original']), selected[0], captured['1']['received_at'][:10])
    assert base.inventory(raw.strict(checked(s3, failed['predecessor']['original']))) == failed['predecessor']['inventory']
    return captured, cutoff, selected


def main():
    s3, lam = boto3.client('s3', region_name='us-east-1'), boto3.client('lambda', region_name='us-east-1')
    scheduler, events = boto3.client('scheduler', region_name='us-east-1'), boto3.client('events', region_name='us-east-1')
    with report('ops_6060_sec_control_trailer_baseline') as r:
        try:
            prior = raw.strict(read(s3, STATUS))
        except Exception as exc:
            if not raw.missing(exc):
                raise
            prior = None
        if prior:
            assert prior['status'] == 'complete', 'Never repeat failed or ambiguous corrected baseline'
            ref = prior['manifest']
            manifest = raw.strict(checked(s3, ref))
            provider_requests = 0
        else:
            old_bytes = read(s3, base.STATUS)
            failed = raw.strict(old_bytes)
            captures, cutoff, selected = adopt(s3, failed)
            failed_ref = retain(s3, old_bytes)
            journal(s3, {'request_id': REQUEST, 'status': 'claimed', 'failed_original_status': failed_ref}, True)
            progress, provider_requests = {'captures': captures, 'failed_original_status': failed_ref}, 0
            try:
                actual = runtime(lam, s3, events, scheduler, FUNCTION)
                related = runtime(lam, s3, events, scheduler, 'justhodl-trade-tickets')
                assert actual == failed['runtime'] and related == failed['related_runtime']
                arn = lam.get_function_configuration(FunctionName=FUNCTION)['FunctionArn']
                observed = bindings(scheduler, events, arn)
                for field in ('schedules', 'classic_default_bus_rules'):
                    assert observed[field] == failed['bindings'][field]
                url = selected[1]
                journal(s3, {'request_id': REQUEST, 'status': 'capturing', 'next_source': url, **progress})
                provider_requests += 1
                value = base.capture(s3, url)
                captures['2'] = value
                journal(s3, {'request_id': REQUEST, 'status': 'capturing', **progress})
                assert value['http_status'] == 200 and value['status'] == 'response_retained', 'Provider error retained; no retry'
                value['inventory'] = sec.inventory(checked(s3, value['original']), url, value['received_at'][:10])
                assert runtime(lam, s3, events, scheduler, FUNCTION) == actual
                assert runtime(lam, s3, events, scheduler, 'justhodl-trade-tickets') == related
                after = bindings(scheduler, events, arn)
                for field in ('schedules', 'classic_default_bus_rules'):
                    assert after[field] == observed[field]
                old_parser = subprocess.check_output(['git', 'show', SOURCE_COMMIT + ':aws/ops/checks/sec_ftd_inventory.py'], cwd=ROOT)
                manifest = {'contract': 'squeeze-settlement-source-baseline.v1', 'request_id': REQUEST, 'generated_at': raw.now(),
                            'commit': SOURCE_COMMIT, 'runtime': actual, 'related_runtime': related, 'bindings': observed,
                            'predecessor': failed['predecessor'], 'captures': captures, 'selection_cutoff': cutoff,
                            'advertised_selected_archives': selected, 'failed_original_status': failed_ref,
                            'original_inventory_parser': retain(s3, old_parser),
                            'inventory_parser': retain(s3, (ROOT / 'aws/ops/checks/sec_ftd_inventory.py').read_bytes()),
                            'reused_provider_responses': 2, 'source_originals_reused_without_recollection': True}
                ref = retain(s3, raw.encoded(manifest))
                journal(s3, {'request_id': REQUEST, 'status': 'complete', 'manifest': ref})
            except Exception as exc:
                journal(s3, {'request_id': REQUEST, 'status': 'failed', 'error_type': type(exc).__name__, **progress})
                r.kv(error_type=type(exc).__name__, provider_requests=provider_requests, retained_progress=progress,
                     engine_invocations=0, public_head_writes=0, private_account_reads=0)
                raise
        protected = {STATUS, base.STATUS, ref['key'], manifest['failed_original_status']['key'],
                     manifest['original_inventory_parser']['key'], manifest['inventory_parser']['key'],
                     manifest['predecessor']['original']['key'], *(v['original']['key'] for v in manifest['captures'].values())}
        def deny(path):
            assert denied_with_retry('https://justhodl.ai/' + path) and denied_with_retry('https://' + BUCKET + '.s3.amazonaws.com/' + path)
        with ThreadPoolExecutor(max_workers=3) as pool:
            for _ in pool.map(deny, sorted(protected)):
                pass
        r.kv(manifest=ref, captures=manifest['captures'], source_commit=manifest['commit'],
             runtime=manifest['runtime'], related_runtime=manifest['related_runtime'],
             bindings=manifest['bindings'], predecessor=manifest['predecessor'],
             reused_provider_responses=2, provider_requests=provider_requests,
             protected_artifacts_checked=len(protected), engine_invocations=0, public_head_writes=0,
             private_account_reads=0, paid_ai_calls=0, notifications_sent=0, portfolio_writes=0, schedules_changed=0)


if __name__ == '__main__':
    try:
        main()
    except Exception:
        sys.exit(1)
