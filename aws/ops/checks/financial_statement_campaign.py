"""Bounded, rate-limited full-population source capture; no model publication."""
from concurrent.futures import ThreadPoolExecutor, wait, FIRST_COMPLETED
from threading import Lock
import hashlib, json, re, time, urllib.request, urllib.error, urllib.parse
from financial_statement_source import request_spec, inspect, strict, ENDPOINTS
from market_runtime_evidence import bounded
PRIVATE = 'audit-private/20260909-originals/financial-statement-research/'
BUCKET = 'justhodl-dashboard-live'
sha = lambda body: hashlib.sha256(body).hexdigest()
encode = lambda value: json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False).encode()


def request_key(request_id, label):
    if not isinstance(request_id, str) or not 1 <= len(request_id) <= 200:
        raise ValueError('Bounded campaign request identity required')
    return PRIVATE + 'requests/' + sha((request_id + ':' + label).encode()) + '.json'


def journal(client, key, doc, claim=False):
    if not re.fullmatch(re.escape(PRIVATE) + r'requests/[a-f0-9]{64}\.json', key):
        raise ValueError('Reviewed campaign journal required')
    body = encode(doc)
    if len(body) > 32 * 1024 * 1024:
        raise ValueError('Whole campaign manifest bound')
    client.put_object(Bucket=BUCKET, Key=key, Body=body, ContentType='application/json', CacheControl='no-store', **({'IfNoneMatch': '*'} if claim else {}))
    assert bounded(client.get_object(Bucket=BUCKET, Key=key)['Body'], 32 * 1024 * 1024) == body


def retain(client, body):
    if not isinstance(body, bytes) or len(body) > 32 * 1024 * 1024:
        raise ValueError('Complete bounded original required')
    ref = {'key': PRIVATE + sha(body) + '.bin', 'sha256': sha(body), 'bytes': len(body)}
    try:
        client.put_object(Bucket=BUCKET, Key=ref['key'], Body=body, ContentType='application/octet-stream', CacheControl='no-store', IfNoneMatch='*')
    except Exception as exc:
        if str(getattr(exc, 'response', {}).get('Error', {}).get('Code')) not in ('409', '412', 'ConditionalRequestConflict', 'PreconditionFailed'):
            raise
    assert read(client, ref) == body
    return ref


def read(client, ref):
    if ref.get('key') != PRIVATE + ref.get('sha256', '') + '.bin' or not re.fullmatch('[a-f0-9]{64}', ref.get('sha256', '')):
        raise ValueError('Exact retained accounting original required')
    body = bounded(client.get_object(Bucket=BUCKET, Key=ref['key'])['Body'], 32 * 1024 * 1024)
    if len(body) != ref['bytes'] or sha(body) != ref['sha256']:
        raise ValueError('Retained original differs')
    return body


def inventory(body, spec, symbols):
    parsed = strict(body)
    if parsed == []:
        return {'rows': 0, 'metadata': [], 'status': 'provider_returned_no_statements',
            'identity_problems': [], 'field_counts': {}, 'null_field_counts': {}, 'zero_field_counts': {},
            'repeated_statement_identities': 0, 'accounting_calculations_qualified': False}
    return {'status': 'provider_rows_retained', **inspect(body, spec, symbols)}


class Rate:
    def __init__(self, interval=0.4):
        self.interval, self.last, self.lock = interval, 0.0, Lock()
    def acquire(self):
        with self.lock:
            time.sleep(max(0, self.last + self.interval - time.monotonic()))
            self.last = time.monotonic()


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *args, **kwargs):
        raise ValueError('Provider redirect refused')


def capture(client, request_id, spec, symbols, credential, rate, now, transport=None):
    assert spec == request_spec(spec['symbol'], spec['endpoint'], spec['period'], symbols)
    status_key = request_key(request_id, spec['url'])
    value = {'request_id': request_id, 'spec': spec, 'status': 'claimed', 'claimed_at': now()}
    journal(client, status_key, value, True)
    try:
        value.update(transport_prepared_at=now(), transport_claimed=True)
        journal(client, status_key, value)
        req = urllib.request.Request(spec['url'] + '&apikey=' + urllib.parse.quote(credential, safe=''),
            headers={'User-Agent': 'JustHodl Research raafouis@gmail.com', 'Accept': 'application/json', 'Accept-Encoding': 'identity'})
        rate.acquire()
        value.update(requested_at=now(), transport_attempted=True)
        try:
            response = (transport or urllib.request.build_opener(NoRedirect()).open)(req, timeout=25)
        except urllib.error.HTTPError as exc:
            response = exc
        code = response.status
        headers = {k.lower(): v for k, v in response.headers.items() if k.lower() in ('content-type', 'content-length', 'date', 'etag', 'last-modified', 'x-ratelimit-limit', 'x-ratelimit-remaining', 'x-ratelimit-reset')}
        body = bounded(response, 16 * 1024 * 1024)
        value.update(status='response_retained', received_at=now(), http_status=code, headers=headers, original=retain(client, body))
        journal(client, status_key, value)
        if code != 200 or not body:
            raise ValueError('Provider unavailable; no retry')
        result = {**value, 'inventory': inventory(body, spec, symbols), 'request_status_key': status_key}
        value.update(status='complete', capture=retain(client, encode(result)))
        journal(client, status_key, value)
        return {**result, 'retained_capture': value['capture']}
    except Exception as exc:
        journal(client, status_key, {**value, 'status': 'failed', 'error_type': type(exc).__name__})
        raise RuntimeError('Statement acquisition failed; complete response/journal retained; never blindly retry') from None


def collect(specs, adopted, fetch, save, workers=3):
    """Stop scheduling after failure; finish and retain only already-started work."""
    if workers not in (1, 2, 3):
        raise ValueError('Reviewed small provider concurrency required')
    keys = [spec['url'] for spec in specs]
    if len(keys) != len(set(keys)) or not set(adopted) <= set(keys):
        raise ValueError('Exact disjoint source-request population required')
    complete, errors = dict(adopted), {}
    remaining = iter(spec for spec in specs if spec['url'] not in complete)
    with ThreadPoolExecutor(max_workers=workers) as pool:
        pending = {}
        def submit():
            spec = next(remaining, None)
            if spec is not None:
                pending[pool.submit(fetch, spec)] = spec
        for _ in range(workers):
            submit()
        while pending:
            finished, _ = wait(pending, return_when=FIRST_COMPLETED)
            for future in finished:
                spec = pending.pop(future)
                try:
                    complete[spec['url']] = future.result()
                except Exception as exc:
                    errors[spec['url']] = type(exc).__name__
            if errors or len(complete) % 25 < len(finished) or not pending:
                save(complete, errors)
            if not errors:
                for _ in finished:
                    submit()
    save(complete, errors)
    if errors or set(complete) != set(keys):
        raise ValueError('Full source capture incomplete; adopt retained successes in a new reviewed campaign')
    return complete
