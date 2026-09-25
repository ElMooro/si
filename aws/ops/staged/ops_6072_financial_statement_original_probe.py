"""Retain twelve original FMP statement responses before changing accounting.

Two explicitly identified issuers, annual/quarterly and all three statements.
This is schema/source diagnosis, not a population or model qualification.
No native or consumer invocation, account read, signal or public write.
"""
from pathlib import Path
import json, re, subprocess, sys, time, urllib.request, urllib.error, urllib.parse
import boto3
ROOT = Path(__file__).resolve().parents[3]
sys.path[:0] = [str(ROOT / p) for p in ('aws/ops', 'aws/ops/staged', 'aws/ops/checks')]
from ops_report import report
from market_runtime_evidence import runtime, bounded
from ops_5998_option_population_retained_acceptance import denied_with_retry
import ops_6071_financial_statement_source_inventory as baseline
import financial_statement_source as source
BUCKET, PRIVATE = baseline.BUCKET, baseline.PRIVATE
BASELINE = {'key': PRIVATE + '593b9b95b218effe401bcf14fcafc65a803400f6f543eeb425896bdf6c16b91f.bin',
    'sha256': '593b9b95b218effe401bcf14fcafc65a803400f6f543eeb425896bdf6c16b91f', 'bytes': 15011}
REQUEST = 'chatgpt-financial-statements-original-6072'
STATUS = PRIVATE + 'requests/' + baseline.sha(REQUEST.encode()) + '.json'
SPECS = tuple(source.request_spec(symbol, endpoint, period) for symbol in ('AAPL', 'JPM')
    for period in ('annual', 'quarter') for endpoint in source.ENDPOINTS)


def journal(s3, key, value, claim=False):
    if not re.fullmatch(re.escape(PRIVATE) + r'requests/[a-f0-9]{64}\.json', key):
        raise ValueError('Reviewed request journal required')
    body = baseline.encoded(value)
    s3.put_object(Bucket=BUCKET, Key=key, Body=body, ContentType='application/json', CacheControl='no-store', **({'IfNoneMatch': '*'} if claim else {}))
    assert bounded(s3.get_object(Bucket=BUCKET, Key=key)['Body']) == body


def original(s3, body):
    if not isinstance(body, bytes) or len(body) > 16 * 1024 * 1024:
        raise ValueError('Complete bounded provider response required')
    ref = {'key': PRIVATE + baseline.sha(body) + '.bin', 'sha256': baseline.sha(body), 'bytes': len(body)}
    try:
        s3.put_object(Bucket=BUCKET, Key=ref['key'], Body=body, ContentType='application/octet-stream', CacheControl='no-store', IfNoneMatch='*')
    except Exception as exc:
        if str(getattr(exc, 'response', {}).get('Error', {}).get('Code')) not in ('409', '412', 'ConditionalRequestConflict', 'PreconditionFailed'):
            raise
    assert baseline.read(s3, ref['key']) == body
    return ref


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *args, **kwargs):
        raise ValueError('Unreviewed provider redirect refused')


def capture(s3, spec, credential, transport=None):
    assert spec in SPECS
    key = PRIVATE + 'requests/' + baseline.sha((REQUEST + ':' + spec['url']).encode()) + '.json'
    record = {'request_id': REQUEST, 'spec': spec, 'status': 'claimed', 'requested_at': baseline.now()}
    journal(s3, key, record, True)
    try:
        # The credential exists only in the outgoing URL in memory. Refuse
        # redirects and never retain/log that URL or raw exception messages.
        request = urllib.request.Request(spec['url'] + '&apikey=' + urllib.parse.quote(credential, safe=''),
            headers={'User-Agent': 'JustHodl Research raafouis@gmail.com', 'Accept': 'application/json', 'Accept-Encoding': 'identity'})
        try:
            response = (transport or urllib.request.build_opener(NoRedirect()).open)(request, timeout=30)
        except urllib.error.HTTPError as exc:
            response = exc
        code = response.status
        headers = {k.lower(): v for k, v in response.headers.items() if k.lower() in ('content-type', 'content-length', 'date', 'etag', 'last-modified')}
        body = bounded(response, 16 * 1024 * 1024)
        record.update(status='response_retained', received_at=baseline.now(), http_status=code,
            headers=headers, original=original(s3, body))
        journal(s3, key, record)
        if code != 200 or not body:
            raise ValueError('Provider response unavailable; original retained, no retry')
        return record, source.inspect(body, spec), key
    except Exception as exc:
        journal(s3, key, {**record, 'status': 'failed', 'error_type': type(exc).__name__})
        raise RuntimeError('Original statement request failed; inspect retained journal; no retry') from None


def main():
    s3, lam = boto3.client('s3', region_name='us-east-1'), boto3.client('lambda', region_name='us-east-1')
    events, scheduler = boto3.client('events', region_name='us-east-1'), boto3.client('scheduler', region_name='us-east-1')
    with report('ops_6072_financial_statement_original_probe') as r:
        subprocess.run([sys.executable, str(ROOT / 'tests/test_financial_statement_source.py')], cwd=ROOT, check=True)
        old = baseline.read(s3, BASELINE['key'])
        assert len(old) == BASELINE['bytes'] and baseline.sha(old) == BASELINE['sha256']
        preserved = json.loads(old)
        universe_ref = preserved['captures']['screener/data.json']['original']
        raw_universe = baseline.read(s3, universe_ref['key'])
        assert len(raw_universe) == universe_ref['bytes'] and baseline.sha(raw_universe) == universe_ref['sha256']
        from financial_statement_inventory import records
        universe = records(json.loads(raw_universe), 'universe')
        assert {'AAPL', 'JPM'} <= {v.get('symbol') or v.get('ticker') for v in universe}
        prior = {fn: runtime(lam, s3, events, scheduler, fn) for fn in baseline.FUNCTIONS}
        assert prior == preserved['runtime']
        cfg = lam.get_function_configuration(FunctionName='justhodl-forensic-screen')
        credential = cfg.get('Environment', {}).get('Variables', {}).get('FMP_KEY')
        if not credential:
            credential = boto3.client('ssm', region_name='us-east-1').get_parameter(Name='/justhodl/fmp/api-key', WithDecryption=True)['Parameter']['Value']
        assert isinstance(credential, str) and credential, 'Existing managed provider credential required'
        progress = {'contract': 'financial-statement-original-diagnostic.v1', 'request_id': REQUEST,
            'generated_at': baseline.now(), 'baseline': BASELINE, 'status': 'claimed', 'captures': {}}
        journal(s3, STATUS, progress, True)
        try:
            source_keys, inventories = [], {}
            for spec in SPECS:
                capture_record, inspected, key = capture(s3, spec, credential)
                name = ':'.join(spec[k] for k in ('symbol', 'period', 'endpoint'))
                progress['captures'][name] = {**capture_record, 'inventory': inspected}
                inventories.setdefault((spec['symbol'], spec['period']), {})[spec['endpoint']] = inspected
                source_keys.append(key); journal(s3, STATUS, progress); time.sleep(0.2)
            progress.update(status='complete', completed_at=baseline.now(),
                alignments={':'.join(key): source.alignment(value) for key, value in inventories.items()},
                provider_requests=12, producer_invocations=0, consumer_invocations=0, private_account_reads=0,
                signal_writes=0, public_writes=0, paid_ai_calls=0, notifications_sent=0, schedules_changed=0,
                population_qualified=False, accounting_calculations_qualified=False, forecast_qualified=False, sizing_qualified=False)
            manifest = original(s3, baseline.encoded(progress))
            protected = {BASELINE['key'], STATUS, manifest['key'], *source_keys,
                *(value['original']['key'] for value in progress['captures'].values())}
            for key in sorted(protected):
                assert denied_with_retry('https://justhodl.ai/' + key) and denied_with_retry('https://' + BUCKET + '.s3.amazonaws.com/' + key)
            assert {fn: runtime(lam, s3, events, scheduler, fn) for fn in baseline.FUNCTIONS} == prior
            journal(s3, STATUS, {**progress, 'manifest': manifest, 'protected_artifacts_checked': len(protected)})
            r.kv(manifest=manifest, captures=progress['captures'], alignments=progress['alignments'],
                protected_artifacts_checked=len(protected), provider_requests=12, producer_invocations=0,
                consumer_invocations=0, private_account_reads=0, signal_writes=0, public_writes=0, paid_ai_calls=0,
                notifications_sent=0, schedules_changed=0, population_qualified=False, accounting_calculations_qualified=False)
        except Exception as exc:
            journal(s3, STATUS, {**progress, 'status': 'failed', 'error_type': type(exc).__name__})
            raise


if __name__ == '__main__':
    try:
        main()
    except Exception:
        sys.exit(1)
