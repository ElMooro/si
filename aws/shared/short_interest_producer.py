"""Native keyless FINRA collection and conditional, replay-checked publication.

All four selected settlements are scanned twice to observe revisions. Failed or
ambiguous source requests are never retried under the same durable identity.
"""
from datetime import datetime, timezone
import gc, re, time, urllib.request, urllib.error
import offexchange_measurements as exact
import short_interest_measurements as measurements
import short_interest_research_model as model
import short_interest_research_store as store


def now():
    return datetime.now(timezone.utc).isoformat()


def missing(exc):
    return str(getattr(exc, 'response', {}).get('Error', {}).get('Code')) in ('404', 'NoSuchKey')


def request_key(request_id):
    if not isinstance(request_id, str) or not 1 <= len(request_id) <= 200:
        raise ValueError('Bounded durable request identity required')
    return model.PRIVATE + 'requests/' + model.sha(request_id.encode()) + '.json'


def raw_read(client, bucket, key):
    return store.bounded(client.get_object(Bucket=bucket, Key=key)['Body'])


def journal(client, bucket, key, value, claim=False):
    if not re.fullmatch(re.escape(model.PRIVATE) + r'requests/[a-f0-9]{64}\.json', key):
        raise ValueError('Reviewed request journal required')
    body = model.encoded(value)
    if len(body) > model.MAX:
        raise ValueError('Whole request journal exceeds bound')
    client.put_object(Bucket=bucket, Key=key, Body=body, ContentType='application/json', CacheControl='no-store', **({'IfNoneMatch': '*'} if claim else {}))
    if raw_read(client, bucket, key) != body:
        raise ValueError('Journal readback differs')


def protect(client, bucket, body):
    if not isinstance(body, bytes) or len(body) > model.MAX:
        raise ValueError('Whole bounded original required')
    ref = {'key': model.PRIVATE + model.sha(body) + '.bin', 'sha256': model.sha(body), 'bytes': len(body)}
    try:
        client.put_object(Bucket=bucket, Key=ref['key'], Body=body, ContentType='application/octet-stream', CacheControl='no-store', IfNoneMatch='*')
    except Exception as exc:
        if not store.conflict(exc):
            raise
    if raw_read(client, bucket, ref['key']) != body:
        raise ValueError('Whole original readback differs')
    return ref


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *args, **kwargs):
        raise ValueError('Unreviewed provider redirect refused')


def fetch(client, bucket, request_id, label, url, body, deadline, transport=None):
    if url not in model.URLS.values() or (url == model.URLS['data']) != (body is not None):
        raise ValueError('Reviewed keyless FINRA request required')
    if body is not None and body != measurements.request(body['compareFilters'][0]['fieldValue'], body['offset']):
        raise ValueError('Complete settlement request required')
    status = request_key(model.sha((request_id + ':' + label).encode()))
    journal(client, bucket, status, {'request_id': request_id, 'source_id': label, 'status': 'claimed', 'url': url, 'body': body}, True)
    started = now()
    try:
        if time.monotonic() >= deadline:
            raise TimeoutError('Source collection budget exhausted')
        request = urllib.request.Request(url, data=model.encoded(body) if body is not None else None,
            headers={'User-Agent': 'JustHodl-ShortInterestResearch/2.0', 'Accept': 'application/json', 'Content-Type': 'application/json'})
        try:
            response = (transport or urllib.request.build_opener(NoRedirect()).open)(request, timeout=max(1, min(20, deadline - time.monotonic())))
        except urllib.error.HTTPError as exc:
            response = exc
        code = response.status
        headers = {k.lower(): v for k, v in response.headers.items() if k.lower() in (
            'record-total', 'record-offset', 'record-limit', 'total-records-on-page', 'record-max-limit',
            'finra-api-request-id', 'content-type', 'content-length', 'date', 'etag', 'last-modified')}
        data = store.bounded(response)
        ref = protect(client, bucket, data)
        result = {'url': url, 'body': body, 'requested_at': started, 'received_at': now(), 'http_status': code, 'headers': headers,
                  'original': ref, 'status': 'response_retained' if code == 200 and data else 'provider_error_retained'}
        journal(client, bucket, status, {'request_id': request_id, 'source_id': label, 'status': 'complete', 'capture': result})
        return result
    except Exception as exc:
        journal(client, bucket, status, {'request_id': request_id, 'source_id': label, 'status': 'failed', 'error_type': type(exc).__name__})
        raise


def successful(client, bucket, captured):
    if captured['status'] != 'response_retained' or captured['http_status'] != 200:
        raise ValueError('FINRA source unavailable; retain current head, never fill or retry')
    return model.original(captured['original'], store.reader(client, bucket))


def collect(client, bucket, request_id, execution_id, deadline, progress, transport=None):
    status = request_key(request_id)
    def save():
        journal(client, bucket, status, {'request_id': request_id, 'execution_id': execution_id, 'status': 'capturing', **progress})
    for kind in ('metadata', 'partitions'):
        progress['captures'][kind] = fetch(client, bucket, request_id, kind, model.URLS[kind], None, deadline, transport)
        save()
        successful(client, bucket, progress['captures'][kind])
    cutoff = model.clock(progress['captures']['partitions']['received_at']).date()
    selected = measurements.partitions(successful(client, bucket, progress['captures']['partitions']), cutoff)
    progress.update(selection_cutoff=cutoff.isoformat(), settlement_plan=selected)
    for spec in selected:
        stamp = spec['settlement_date']
        fingerprints = []
        for pass_number in (1, 2):
            offset, total, pages, rows = 0, None, 0, []
            while True:
                if pages >= 40:
                    raise ValueError('Complete population exceeds reviewed page bound')
                label = f'settlement:{stamp}:pass:{pass_number}:offset:{offset}'
                body = measurements.request(stamp, offset)
                captured = fetch(client, bucket, request_id, label, model.URLS['data'], body, deadline, transport)
                progress['captures'][label] = captured
                save()
                if sum(v['original']['bytes'] for v in progress['captures'].values()) > 192 * 1024 * 1024:
                    raise ValueError('Whole campaign source byte bound exceeded')
                data = successful(client, bucket, captured)
                page = exact.page(data, captured['headers'], offset, measurements.LIMIT)
                if total is not None and total != page['reported_total']:
                    raise ValueError('FINRA population changed while paging')
                total = page['reported_total']
                rows.extend(measurements.rows(data, stamp))
                offset = page['next_offset']
                pages += 1
                if page['reported_end_reached']:
                    break
            if not total or len(rows) != total:
                raise ValueError('Nonempty whole settlement population required')
            fingerprints.append((total, measurements.fingerprint(rows)))
            del rows
        if fingerprints[0] != fingerprints[1]:
            raise ValueError('Two complete FINRA scans disagree')
    return {k: progress[k] for k in ('captures', 'selection_cutoff', 'settlement_plan', 'predecessor')}


def not_older(packet, previous):
    if previous.get('generated_at') and model.clock(packet['generated_at']) <= model.clock(previous['generated_at']):
        return False
    if previous.get('settlement_date') and packet['settlement_date'] < exact.day(previous['settlement_date']):
        return False
    return True


def run(client, bucket, request_id, execution_id, remaining_seconds=300, transport=None):
    if not isinstance(execution_id, str) or not execution_id:
        raise ValueError('Actual AWS execution identity required')
    if remaining_seconds < 250:
        raise ValueError('Insufficient collection and replay budget')
    status = request_key(request_id)
    try:
        existing = model.strict(raw_read(client, bucket, status))
    except Exception as exc:
        if not missing(exc):
            raise
        existing = None
    if existing:
        if existing.get('status') != 'complete':
            raise ValueError('Request already attempted; inspect retained failure or ambiguous state')
        return existing['result']
    journal(client, bucket, status, {'request_id': request_id, 'execution_id': execution_id, 'status': 'claimed', 'started_at': now()}, True)
    progress = {'captures': {}}
    try:
        head = client.get_object(Bucket=bucket, Key=model.CURRENT)
        old_bytes = store.bounded(head['Body'])
        previous, etag = model.strict(old_bytes), head['ETag']
        progress['predecessor'] = protect(client, bucket, old_bytes)
        deadline = time.monotonic() + min(130, remaining_seconds - 160)
        inputs = collect(client, bucket, request_id, execution_id, deadline, progress, transport)
        inputs.update(contract='short-interest-original-inputs.v1', generated_at=now(), request_id=request_id,
                      execution_id=execution_id, collection_policy='two_complete_scans_of_latest_four_advertised_settlements')
        compiled = model.compile_output(inputs, store.reader(client, bucket))
        ref = store.retain(client, bucket, inputs, compiled)
        expected = model.digest(compiled['packet'])
        del compiled
        gc.collect()
        replayed = store.replay(ref, store.reader(client, bucket))
        packet = replayed['packet']
        if model.digest(packet) != expected:
            raise ValueError('Retained provider-source reconstruction differs')
        if not not_older(packet, previous):
            result = {'published': False, 'reason': 'observation_or_publication_rollback', 'replay': ref}
        else:
            published = model.encoded({**packet, 'replay': ref})
            if len(published) > model.MAX:
                raise ValueError('Whole native head exceeds bound')
            try:
                client.put_object(Bucket=bucket, Key=model.CURRENT, Body=published, ContentType='application/json', CacheControl='no-store', IfMatch=etag)
            except Exception as exc:
                if not store.conflict(exc):
                    raise
                result = {'published': False, 'reason': 'concurrent_publication', 'replay': ref}
            else:
                if raw_read(client, bucket, model.CURRENT) != published:
                    raise ValueError('Native publication readback differs')
                result = {'published': True, 'generated_at': packet['generated_at'], 'counts': packet['counts'], 'replay': ref}
        journal(client, bucket, status, {'request_id': request_id, 'execution_id': execution_id, 'status': 'complete', 'result': result})
        return result
    except Exception as exc:
        journal(client, bucket, status, {'request_id': request_id, 'execution_id': execution_id, 'status': 'failed', 'error_type': type(exc).__name__, **progress})
        raise
