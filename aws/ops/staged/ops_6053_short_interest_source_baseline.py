"""Retain short-interest predecessor and official schema/partition discoveries.

No legacy producer invocation, pricing call, account access or public head write.
Two explicit public FINRA requests establish the next source campaign's shape;
they do not establish population coverage or validate old signal classifications.
"""
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
from collections import Counter
import hashlib, re, sys, urllib.request, urllib.error
import boto3
ROOT = Path(__file__).resolve().parents[3]
sys.path[:0] = [str(ROOT / p) for p in ('aws/ops', 'aws/ops/staged', 'aws/ops/checks')]
from ops_report import report
from market_runtime_evidence import runtime
from ops_5998_option_population_retained_acceptance import denied_with_retry
from ops_6050_short_volume_schedule_baseline import schedule_row
import ops_6036_offexchange_whole_source_preflight as raw
BUCKET = raw.BUCKET
FUNCTION = 'justhodl-short-interest'
CURRENT = 'data/short-interest.json'
PRIVATE = 'audit-private/20260909-originals/short-interest-research/'
REQUEST = 'chatgpt-short-interest-source-baseline-6053'
STATUS = PRIVATE + 'requests/' + raw.sha(REQUEST.encode()) + '.json'
URLS = {kind: 'https://api.finra.org/' + kind + '/group/otcMarket/name/consolidatedShortInterest'
        for kind in ('metadata', 'partitions')}


def read(s3, key):
    if key != CURRENT and not re.fullmatch(re.escape(PRIVATE) + r'(?:[a-f0-9]{64}\.bin|requests/[a-f0-9]{64}\.json)', key):
        raise ValueError('Reviewed public-market source or protected evidence required')
    return raw.bounded(s3.get_object(Bucket=BUCKET, Key=key)['Body'], 8 * 1024 * 1024)


def retain(s3, body):
    if not isinstance(body, bytes) or not 0 < len(body) <= 8 * 1024 * 1024:
        raise ValueError('Complete bounded source required')
    ref = {'key': PRIVATE + raw.sha(body) + '.bin', 'sha256': raw.sha(body), 'bytes': len(body)}
    try:
        s3.put_object(Bucket=BUCKET, Key=ref['key'], Body=body, ContentType='application/octet-stream', CacheControl='no-store', IfNoneMatch='*')
    except Exception as exc:
        if str(getattr(exc, 'response', {}).get('Error', {}).get('Code')) not in ('409', '412', 'ConditionalRequestConflict', 'PreconditionFailed'):
            raise
    assert read(s3, ref['key']) == body
    return ref


def checked(s3, ref):
    assert ref['key'] == PRIVATE + ref['sha256'] + '.bin'
    body = read(s3, ref['key'])
    assert len(body) == ref['bytes'] and raw.sha(body) == ref['sha256']
    return body


def journal(s3, doc, claim=False):
    body = raw.encoded(doc)
    s3.put_object(Bucket=BUCKET, Key=STATUS, Body=body, ContentType='application/json', CacheControl='no-store',
                  **({'IfNoneMatch': '*'} if claim else {}))
    assert read(s3, STATUS) == body


def inventory(packet):
    rows = packet.get('by_ticker')
    if not isinstance(rows, dict):
        raise ValueError('Expected complete predecessor by_ticker mapping')
    if any(not isinstance(v, dict) for v in rows.values()):
        raise ValueError('Malformed predecessor record')
    count = lambda field: sum(row.get(field) is not None for row in rows.values())
    clocks = lambda field: dict(sorted(Counter(str(row.get(field)) for row in rows.values()).items()))
    return {'generated_at': packet.get('generated_at'), 'version': packet.get('version'),
            'measurement_contract': packet.get('measurement_contract'), 'symbols': len(rows),
            'fields': sorted({key for row in rows.values() for key in row}),
            'non_null_counts': {key: count(key) for key in ('short_interest', 'days_to_cover', 'si_change_pct',
                                                          'short_float_pct', 'daily_short_volume_pct', 'price_change_pct')},
            'observation_dates': {key: clocks(key) for key in ('settlement_date', 'short_float_as_of', 'daily_short_volume_as_of')},
            'source_counts': {key: clocks(key) for key in ('short_interest_source', 'days_to_cover_source', 'short_float_source')},
            'legacy_signal_counts': clocks('signal'),
            'price_window_days': clocks('price_window_days'),
            'provider_originals_verified': False, 'historical_availability_verified': False,
            'legacy_signal_qualification': False}


def bindings(scheduler, events, arn):
    rows, rules, groups, scanned = [], [], [], 0
    for page in scheduler.get_paginator('list_schedule_groups').paginate():
        for group in page['ScheduleGroups']:
            groups.append(group['Name'])
            for chunk in scheduler.get_paginator('list_schedules').paginate(GroupName=group['Name']):
                for item in chunk.get('Schedules', []):
                    scanned += 1
                    target = item.get('Target', {}).get('Arn', '')
                    if target == arn or target.startswith(arn + ':'):
                        rows.append(schedule_row(scheduler.get_schedule(Name=item['Name'], GroupName=group['Name']), arn))
    for page in events.get_paginator('list_rule_names_by_target').paginate(TargetArn=arn):
        for name in page['RuleNames']:
            rule = events.describe_rule(Name=name)
            targets = events.list_targets_by_rule(Rule=name)['Targets']
            matched = [t for t in targets if t.get('Arn') == arn]
            assert matched
            rules.append({'name': name, 'state': rule['State'], 'expression': rule.get('ScheduleExpression'),
                          'target_arn': arn, 'input_sha256': [hashlib.sha256(t.get('Input', '').encode()).hexdigest() for t in matched]})
    return {'schedules': sorted(rows, key=lambda v: (v['group'], v['name'])), 'classic_default_bus_rules': sorted(rules, key=lambda v: v['name']),
            'groups_scanned': sorted(groups), 'schedulers_scanned': scanned, 'other_invocation_paths_excluded_from_claim': True}


def capture(s3, kind, transport=None):
    url = URLS[kind]
    started = raw.now()
    request = urllib.request.Request(url, headers={'User-Agent': 'JustHodl-ShortInterestSourceAudit/1.0', 'Accept': 'application/json'})
    try:
        response = (transport or urllib.request.build_opener(raw.NoRedirect()).open)(request, timeout=35)
    except urllib.error.HTTPError as exc:
        response = exc
    code = response.status
    headers = {k.lower(): v for k, v in response.headers.items() if k.lower() in ('content-type', 'content-length', 'date', 'etag', 'last-modified')}
    body = raw.bounded(response, 8 * 1024 * 1024)
    ref = retain(s3, body)
    return {'url': url, 'requested_at': started, 'received_at': raw.now(), 'http_status': code,
            'headers': headers, 'original': ref, 'discovery': raw.strict(body) if code == 200 else None,
            'status': 'response_retained' if code == 200 else 'provider_error_retained', 'population_qualified': False}


def main():
    s3, lam = boto3.client('s3', region_name='us-east-1'), boto3.client('lambda', region_name='us-east-1')
    scheduler, events = boto3.client('scheduler', region_name='us-east-1'), boto3.client('events', region_name='us-east-1')
    with report('ops_6053_short_interest_source_baseline') as r:
        try:
            prior = raw.strict(read(s3, STATUS))
        except Exception as exc:
            if not raw.missing(exc):
                raise
            prior = None
        if prior:
            assert prior['status'] == 'complete', 'Inspect failed/ambiguous baseline; never repeat source requests'
            ref = prior['manifest']
            manifest = raw.strict(checked(s3, ref))
        else:
            journal(s3, {'request_id': REQUEST, 'status': 'claimed', 'started_at': raw.now()}, True)
            progress = {'captures': {}}
            try:
                actual = runtime(lam, s3, events, scheduler, FUNCTION)
                arn = lam.get_function_configuration(FunctionName=FUNCTION)['FunctionArn']
                observed_schedules = bindings(scheduler, events, arn)
                previous = read(s3, CURRENT)
                progress.update(runtime=actual, bindings=observed_schedules,
                                predecessor={'original': retain(s3, previous), 'inventory': inventory(raw.strict(previous))})
                for kind in URLS:
                    journal(s3, {'request_id': REQUEST, 'status': 'capturing', 'next_source': kind, **progress})
                    progress['captures'][kind] = capture(s3, kind)
                    journal(s3, {'request_id': REQUEST, 'status': 'capturing', **progress})
                assert runtime(lam, s3, events, scheduler, FUNCTION) == actual
                manifest = {'contract': 'short-interest-source-baseline.v1', 'generated_at': raw.now(), 'request_id': REQUEST, **progress}
                ref = retain(s3, raw.encoded(manifest))
                journal(s3, {'request_id': REQUEST, 'status': 'complete', 'manifest': ref})
            except Exception as exc:
                journal(s3, {'request_id': REQUEST, 'status': 'failed', 'error_type': type(exc).__name__, **progress})
                raise
        protected = {STATUS, ref['key'], manifest['predecessor']['original']['key'],
                     *(v['original']['key'] for v in manifest['captures'].values())}
        def deny(key):
            assert denied_with_retry('https://justhodl.ai/' + key) and denied_with_retry('https://' + BUCKET + '.s3.amazonaws.com/' + key)
        with ThreadPoolExecutor(max_workers=3) as pool:
            for _ in pool.map(deny, protected):
                pass
        r.kv(manifest=ref, runtime=manifest['runtime'], bindings=manifest['bindings'], predecessor=manifest['predecessor'],
             captures=manifest['captures'], protected_artifacts_checked=len(protected), provider_requests=2,
             engine_invocations=0, public_head_writes=0, private_account_reads=0, paid_ai_calls=0,
             notifications_sent=0, portfolio_writes=0, schedules_changed=0)


if __name__ == '__main__':
    try:
        main()
    except Exception:
        sys.exit(1)
