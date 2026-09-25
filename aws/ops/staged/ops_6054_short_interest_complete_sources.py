"""Retain two complete scans of four official short-interest settlements.

No legacy handler, private account, pricing source, notification or current-head
write. Accepted/failed campaigns are never silently recollected.
"""
from pathlib import Path
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor
import hashlib, sys, time, urllib.request, urllib.error
import boto3
ROOT = Path(__file__).resolve().parents[3]
sys.path[:0] = [str(ROOT / p) for p in ('aws/ops', 'aws/ops/staged', 'aws/ops/checks', 'aws/shared')]
from ops_report import report
from market_runtime_evidence import runtime
from ops_5998_option_population_retained_acceptance import denied_with_retry
import ops_6053_short_interest_source_baseline as base
import short_interest_inventory as inventory
import offexchange_measurements as exact
raw = base.raw
BUCKET, PRIVATE = base.BUCKET, base.PRIVATE
REQUEST = 'chatgpt-short-interest-complete-sources-6054'
BASELINE = {'key': PRIVATE + '2b166e05d09ea599c6408ca5c031a26c0a5b67d5df4953d7fa8442514d34e579.bin',
            'sha256': '2b166e05d09ea599c6408ca5c031a26c0a5b67d5df4953d7fa8442514d34e579', 'bytes': 13545}
DATA = 'https://api.finra.org/data/group/otcMarket/name/consolidatedShortInterest'


def key(label):
    return PRIVATE + 'requests/' + raw.sha((REQUEST + ':' + label).encode()) + '.json'


def journal(s3, label, value, claim=False):
    body = raw.encoded(value)
    s3.put_object(Bucket=BUCKET, Key=key(label), Body=body, ContentType='application/json', CacheControl='no-store', **({'IfNoneMatch': '*'} if claim else {}))
    assert base.read(s3, key(label)) == body


def fetch(s3, label, url, body, deadline, transport=None):
    if url not in (*base.URLS.values(), DATA) or (url == DATA) != (body is not None):
        raise ValueError('Reviewed keyless FINRA request required')
    if body is not None:
        assert body == inventory.request(body['compareFilters'][0]['fieldValue'], body['offset'])
    journal(s3, label, {'request_id': REQUEST, 'status': 'claimed', 'url': url, 'body': body}, True)
    started = raw.now()
    try:
        if time.monotonic() >= deadline:
            raise TimeoutError('Source campaign deadline')
        request = urllib.request.Request(url, data=raw.encoded(body) if body is not None else None,
            headers={'User-Agent': 'JustHodl-ShortInterestResearch/2.0', 'Accept': 'application/json', 'Content-Type': 'application/json'})
        try:
            response = (transport or urllib.request.build_opener(raw.NoRedirect()).open)(request, timeout=max(1, min(40, deadline - time.monotonic())))
        except urllib.error.HTTPError as exc:
            response = exc
        code = response.status
        headers = {k.lower(): v for k, v in response.headers.items() if k.lower() in (*raw.HEADERS, 'date', 'last-modified', 'etag', 'content-length')}
        body_bytes = raw.bounded(response, 8 * 1024 * 1024)
        ref = base.retain(s3, body_bytes)
        result = {'url': url, 'body': body, 'requested_at': started, 'received_at': raw.now(), 'http_status': code,
                  'headers': headers, 'original': ref, 'status': 'response_retained' if code == 200 else 'provider_error_retained'}
        journal(s3, label, {'request_id': REQUEST, 'status': 'complete', 'capture': result})
        return result
    except Exception as exc:
        journal(s3, label, {'request_id': REQUEST, 'status': 'failed', 'error_type': type(exc).__name__})
        raise


def successful(s3, capture):
    if capture['http_status'] != 200 or capture['status'] != 'response_retained':
        raise ValueError('Provider response failed; inspect retained evidence')
    return base.checked(s3, capture['original'])


def scan(s3, stamp, pass_number, deadline, captures, progress):
    offset, total, records, page_count = 0, None, [], 0
    while True:
        if page_count >= 40:
            raise ValueError('Full-population page bound exceeded; never truncate')
        body = inventory.request(stamp, offset)
        label = f'settlement:{stamp}:pass:{pass_number}:offset:{offset}'
        capture = fetch(s3, label, DATA, body, deadline)
        captures[label] = capture
        journal(s3, 'campaign', {'request_id': REQUEST, 'status': 'capturing', **progress})
        if sum(value['original']['bytes'] for value in captures.values()) > 192 * 1024 * 1024:
            raise ValueError('Whole-source campaign byte bound exceeded')
        source = successful(s3, capture)
        page = exact.page(source, capture['headers'], offset, inventory.LIMIT)
        if total is not None and total != page['reported_total']:
            raise ValueError('Provider total changed within collection')
        total = page['reported_total']
        records.extend(inventory.rows(source, stamp))
        page_count += 1
        offset = page['next_offset']
        if page['reported_end_reached']:
            break
    if not total or len(records) != total:
        raise ValueError('Nonempty complete partition required')
    return records, {'reported_total': total, 'rows_collected': len(records), 'pages': page_count,
                     'population_sha256': inventory.fingerprint(records), 'snapshot_atomic': False}


def main():
    s3, lam = boto3.client('s3', region_name='us-east-1'), boto3.client('lambda', region_name='us-east-1')
    scheduler, events = boto3.client('scheduler', region_name='us-east-1'), boto3.client('events', region_name='us-east-1')
    with report('ops_6054_short_interest_complete_sources') as r:
        try:
            prior = raw.strict(base.read(s3, key('campaign')))
        except Exception as exc:
            if not raw.missing(exc):
                raise
            prior = None
        if prior:
            assert prior['status'] == 'complete', 'Inspect failed/ambiguous campaign; no retry'
            ref = prior['manifest']
            manifest = raw.strict(base.checked(s3, ref))
        else:
            baseline = raw.strict(base.checked(s3, BASELINE))
            assert baseline['contract'] == 'short-interest-source-baseline.v1'
            journal(s3, 'campaign', {'request_id': REQUEST, 'status': 'claimed', 'started_at': raw.now()}, True)
            progress = {'captures': {}, 'settlements': {}}
            try:
                actual = runtime(lam, s3, events, scheduler, base.FUNCTION)
                assert actual == baseline['runtime'], 'Native source changed since baseline'
                deadline = time.monotonic() + 600
                for kind in base.URLS:
                    capture = fetch(s3, kind, base.URLS[kind], None, deadline)
                    progress['captures'][kind] = capture
                    journal(s3, 'campaign', {'request_id': REQUEST, 'status': 'capturing', **progress})
                metadata = raw.strict(successful(s3, progress['captures']['metadata']))
                assert metadata == baseline['captures']['metadata']['discovery'], 'Official schema changed'
                plan = inventory.partitions(successful(s3, progress['captures']['partitions']), datetime.now(timezone.utc).date())
                progress['settlement_plan'] = plan
                for spec in plan:
                    stamp = spec['settlement_date']
                    first, scan_one = scan(s3, stamp, 1, deadline, progress['captures'], progress)
                    second, scan_two = scan(s3, stamp, 2, deadline, progress['captures'], progress)
                    assert scan_one == scan_two, 'Two complete provider scans disagree; no qualification'
                    progress['settlements'][stamp] = {**spec, 'scans': [scan_one, scan_two],
                        'two_complete_scans_equal': True, 'inventory': inventory.describe(first)}
                    del first, second
                    journal(s3, 'campaign', {'request_id': REQUEST, 'status': 'capturing', **progress})
                assert runtime(lam, s3, events, scheduler, base.FUNCTION) == actual
                manifest = {'contract': 'short-interest-complete-sources.v1', 'generated_at': raw.now(), 'request_id': REQUEST,
                    'baseline': BASELINE, 'runtime': actual, **progress,
                    'provider_requests': len(progress['captures']),
                    'provider_bytes': sum(v['original']['bytes'] for v in progress['captures'].values()),
                    'source_modules': {name: raw.sha((ROOT / path).read_bytes()) for name, path in (
                        ('short_interest_inventory', 'aws/ops/checks/short_interest_inventory.py'),
                        ('offexchange_measurements', 'aws/shared/offexchange_measurements.py'))},
                    'population_arithmetic_qualified': False, 'forecast_qualified': False, 'sizing_qualified': False,
                    'historical_availability_verified': False, 'snapshot_atomic': False}
                ref = base.retain(s3, raw.encoded(manifest))
                journal(s3, 'campaign', {'request_id': REQUEST, 'status': 'complete', 'manifest': ref})
            except Exception as exc:
                journal(s3, 'campaign', {'request_id': REQUEST, 'status': 'failed', 'error_type': type(exc).__name__, **progress})
                raise
        protected = {key('campaign'), ref['key'], BASELINE['key']}
        for label, capture in manifest['captures'].items():
            protected.update((key(label), capture['original']['key']))
        def deny(path):
            assert denied_with_retry('https://justhodl.ai/' + path) and denied_with_retry('https://' + BUCKET + '.s3.amazonaws.com/' + path)
        with ThreadPoolExecutor(max_workers=4) as pool:
            for _ in pool.map(deny, sorted(protected)):
                pass
        r.kv(manifest=ref, settlements=manifest['settlements'], provider_requests=manifest['provider_requests'],
             provider_bytes=manifest['provider_bytes'], source_modules=manifest['source_modules'],
             protected_artifacts_checked=len(protected), engine_invocations=0, public_head_writes=0,
             private_account_reads=0, paid_ai_calls=0, notifications_sent=0, portfolio_writes=0, schedules_changed=0)


if __name__ == '__main__':
    try:
        main()
    except Exception:
        sys.exit(1)
