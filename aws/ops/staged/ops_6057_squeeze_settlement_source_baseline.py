"""Retain the full Squeeze Fuel predecessor and two advertised SEC CNS archives.

No producer invocation, pricing/AI API, account read or public packet mutation.
The archive inventory validates shape, not forced buying or a squeeze forecast.
"""
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
from collections import Counter
from datetime import datetime
import re, subprocess, sys, time, urllib.request, urllib.error
import boto3
ROOT = Path(__file__).resolve().parents[3]
sys.path[:0] = [str(ROOT / p) for p in ('aws/ops', 'aws/ops/staged', 'aws/ops/checks')]
from ops_report import report
from market_runtime_evidence import runtime
from ops_5998_option_population_retained_acceptance import denied_with_retry
from ops_6053_short_interest_source_baseline import bindings
import ops_6036_offexchange_whole_source_preflight as raw
import sec_ftd_inventory as sec
BUCKET = raw.BUCKET
FUNCTION = 'justhodl-squeeze-fuel'
CURRENT = 'data/squeeze-fuel.json'
PRIVATE = 'audit-private/20260909-originals/sec-ftd-research/'
REQUEST = 'chatgpt-squeeze-settlement-baseline-6057'
STATUS = PRIVATE + 'requests/' + raw.sha(REQUEST.encode()) + '.json'


def source_commit():
    # Report-only and scheduled registry commits can advance main after deploy.
    return subprocess.check_output(['git', 'log', '-1', '--format=%H', '--',
        'aws/ops/staged/ops_6057_squeeze_settlement_source_baseline.py'], cwd=ROOT, text=True).strip()


def read(s3, key):
    if key != CURRENT and not re.fullmatch(re.escape(PRIVATE) + r'(?:[a-f0-9]{64}\.bin|requests/[a-f0-9]{64}\.json)', key):
        raise ValueError('Reviewed source or protected evidence read required')
    return raw.bounded(s3.get_object(Bucket=BUCKET, Key=key)['Body'], sec.MAX_ZIP)


def retain(s3, body):
    if not isinstance(body, bytes) or not 0 < len(body) <= sec.MAX_ZIP:
        raise ValueError('Complete bounded original required')
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


def journal(s3, value, claim=False):
    body = raw.encoded(value)
    assert len(body) <= sec.MAX_ZIP
    s3.put_object(Bucket=BUCKET, Key=STATUS, Body=body, ContentType='application/json', CacheControl='no-store',
                  **({'IfNoneMatch': '*'} if claim else {}))
    assert read(s3, STATUS) == body


def inventory(packet):
    if not isinstance(packet, dict) or not isinstance(packet.get('board', []), list):
        raise ValueError('Whole preceding public research packet required')
    rows = packet.get('board', [])
    assert all(isinstance(row, dict) for row in rows)
    clocks = lambda field: dict(sorted(Counter(str(row.get(field)) for row in rows).items()))
    return {'generated_at': packet.get('generated_at'), 'version': packet.get('version'), 'ok': packet.get('ok'),
            'n_scored': packet.get('n_scored'), 'board_rows': len(rows), 'top_picks': len(packet.get('top_picks', [])),
            'si_settlement_date': packet.get('si_settlement_date'), 'ftd_file': packet.get('ftd_file'),
            'row_fields': sorted({key for row in rows for key in row}), 'settlements': clocks('settlement_date'),
            'legacy_states': clocks('state'), 'rows_with_retained_sec_balance': sum('ftd' in r or 'last_fails' in r or 'max_fails' in r for r in rows),
            'source_originals_verified': False, 'legacy_forecast_qualified': False, 'sizing_qualified': False}


def capture(s3, url, transport=None):
    if url != sec.INDEX:
        sec.archive_period(url)
    started = raw.now()
    request = urllib.request.Request(url, headers={'User-Agent': 'JustHodl Research raafouis@gmail.com',
                                                  'Accept': 'text/html' if url == sec.INDEX else 'application/zip, application/octet-stream'})
    try:
        response = (transport or urllib.request.build_opener(raw.NoRedirect()).open)(request, timeout=35)
    except urllib.error.HTTPError as exc:
        response = exc
    code = response.status
    headers = {k.lower(): v for k, v in response.headers.items() if k.lower() in ('content-type', 'content-length', 'date', 'etag', 'last-modified')}
    body = raw.bounded(response, sec.MAX_ZIP)
    ref = retain(s3, body)
    return {'url': url, 'requested_at': started, 'received_at': raw.now(), 'http_status': code,
            'headers': headers, 'original': ref, 'status': 'response_retained' if code == 200 else 'provider_error_retained'}


def main():
    s3, lam = boto3.client('s3', region_name='us-east-1'), boto3.client('lambda', region_name='us-east-1')
    scheduler, events = boto3.client('scheduler', region_name='us-east-1'), boto3.client('events', region_name='us-east-1')
    with report('ops_6057_squeeze_settlement_source_baseline') as r:
        try:
            prior = raw.strict(read(s3, STATUS))
        except Exception as exc:
            if not raw.missing(exc):
                raise
            prior = None
        if prior:
            assert prior['status'] == 'complete', 'Inspect failed or ambiguous baseline; never repeat provider requests'
            ref = prior['manifest']
            manifest = raw.strict(checked(s3, ref))
            provider_requests = 0
        else:
            journal(s3, {'request_id': REQUEST, 'status': 'claimed', 'started_at': raw.now()}, True)
            progress, provider_requests = {'captures': {}}, 0
            try:
                actual = runtime(lam, s3, events, scheduler, FUNCTION)
                related = runtime(lam, s3, events, scheduler, 'justhodl-trade-tickets')
                commit = source_commit()
                for observed in (actual, related):
                    assert observed['receipt'] == {'status': 'matched', 'commit': commit}, 'Exact deployed source receipt required'
                arn = lam.get_function_configuration(FunctionName=FUNCTION)['FunctionArn']
                observed_schedules = bindings(scheduler, events, arn)
                previous = read(s3, CURRENT)
                progress.update(commit=commit, runtime=actual, related_runtime=related, bindings=observed_schedules,
                                predecessor={'original': retain(s3, previous), 'inventory': inventory(raw.strict(previous))})
                urls = [sec.INDEX]
                for i in range(3):
                    url = urls[i]
                    journal(s3, {'request_id': REQUEST, 'status': 'capturing', 'next_source': url, **progress})
                    provider_requests += 1
                    captured = capture(s3, url)
                    progress['captures'][str(i)] = captured
                    journal(s3, {'request_id': REQUEST, 'status': 'capturing', **progress})
                    assert captured['http_status'] == 200, 'Provider error retained; no retry'
                    body = checked(s3, captured['original'])
                    if i == 0:
                        cutoff = datetime.fromisoformat(captured['received_at'].replace('Z', '+00:00')).date().isoformat()
                        selected = sec.advertised_archives(body, cutoff, 2)
                        urls.extend(selected)
                        progress.update(selection_cutoff=cutoff, advertised_selected_archives=selected)
                    else:
                        captured['inventory'] = sec.inventory(body, url, captured['received_at'][:10])
                    time.sleep(0.2)
                assert runtime(lam, s3, events, scheduler, FUNCTION) == actual
                assert runtime(lam, s3, events, scheduler, 'justhodl-trade-tickets') == related
                final_bindings = bindings(scheduler, events, arn)
                for field in ('schedules', 'classic_default_bus_rules'):
                    assert final_bindings[field] == observed_schedules[field]
                parser = ROOT / 'aws/ops/checks/sec_ftd_inventory.py'
                manifest = {'contract': 'squeeze-settlement-source-baseline.v1', 'generated_at': raw.now(), 'request_id': REQUEST,
                            'inventory_parser': retain(s3, parser.read_bytes()), **progress}
                ref = retain(s3, raw.encoded(manifest))
                journal(s3, {'request_id': REQUEST, 'status': 'complete', 'manifest': ref})
            except Exception as exc:
                journal(s3, {'request_id': REQUEST, 'status': 'failed', 'error_type': type(exc).__name__, **progress})
                r.kv(error_type=type(exc).__name__, retained_progress=progress, provider_requests=provider_requests,
                     engine_invocations=0, public_head_writes=0, private_account_reads=0, notifications_sent=0)
                raise
        protected = {STATUS, ref['key'], manifest['predecessor']['original']['key'], manifest['inventory_parser']['key'],
                     *(v['original']['key'] for v in manifest['captures'].values())}
        def deny(key):
            assert denied_with_retry('https://justhodl.ai/' + key) and denied_with_retry('https://' + BUCKET + '.s3.amazonaws.com/' + key)
        with ThreadPoolExecutor(max_workers=3) as pool:
            for _ in pool.map(deny, protected):
                pass
        r.kv(manifest=ref, commit=manifest['commit'], runtime=manifest['runtime'], related_runtime=manifest['related_runtime'],
             bindings=manifest['bindings'], predecessor=manifest['predecessor'],
             captures=manifest['captures'], selected_archives=manifest['advertised_selected_archives'],
             protected_artifacts_checked=len(protected), provider_requests=provider_requests,
             engine_invocations=0, public_head_writes=0, private_account_reads=0, paid_ai_calls=0,
             notifications_sent=0, portfolio_writes=0, schedules_changed=0)


if __name__ == '__main__':
    try:
        main()
    except Exception:
        sys.exit(1)
