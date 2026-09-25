"""Keyless SEC acquisition and conditional, replay-checked native publication.

Retains complete index/ZIP responses and the whole previous packet. Every
request is durably claimed before transport; failed or ambiguous identities
are never retried. No accounts, AI, notifications or orders are accessible.
"""
from datetime import datetime, timezone, date
import gc, re, time, urllib.request, urllib.error
import sec_ftd_source as source
import sec_ftd_research_model as model
import sec_ftd_research_store as store


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
    client.put_object(Bucket=bucket, Key=key, Body=body, ContentType='application/json', CacheControl='no-store',
                      **({'IfNoneMatch': '*'} if claim else {}))
    if raw_read(client, bucket, key) != body:
        raise ValueError('Journal readback differs')


def protect(client, bucket, body):
    if not isinstance(body, bytes) or len(body) > model.MAX:
        raise ValueError('Whole bounded original required')
    ref = {'key': model.PRIVATE + model.sha(body) + '.bin', 'sha256': model.sha(body), 'bytes': len(body)}
    try:
        client.put_object(Bucket=bucket, Key=ref['key'], Body=body, ContentType='application/octet-stream',
                          CacheControl='no-store', IfNoneMatch='*')
    except Exception as exc:
        if not store.conflict(exc):
            raise
    if raw_read(client, bucket, ref['key']) != body:
        raise ValueError('Whole original readback differs')
    return ref


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *args, **kwargs):
        raise ValueError('Unreviewed provider redirect refused')


def fetch(client, bucket, request_id, label, url, deadline, transport=None):
    if url != source.INDEX:
        source.archive_period(url)
    status = request_key(model.sha((request_id + ':' + label).encode()))
    journal(client, bucket, status, {'request_id': request_id, 'source_id': label, 'status': 'claimed', 'url': url}, True)
    started = now()
    try:
        if time.monotonic() >= deadline:
            raise TimeoutError('Source collection budget exhausted')
        request = urllib.request.Request(url, headers={'User-Agent': 'JustHodl Research raafouis@gmail.com',
            'Accept': 'text/html,application/zip,application/octet-stream', 'Accept-Encoding': 'identity'})
        try:
            response = (transport or urllib.request.build_opener(NoRedirect()).open)(request,
                timeout=max(1, min(25, deadline - time.monotonic())))
        except urllib.error.HTTPError as exc:
            response = exc
        code = response.status
        headers = {k.lower(): v for k, v in response.headers.items() if k.lower() in (
            'content-type', 'content-length', 'date', 'etag', 'last-modified')}
        data = store.bounded(response)
        # Even an empty error is retained as exact bytes. Empty successful
        # bodies cannot qualify, and never replace the previous native packet.
        ref = protect(client, bucket, data)
        result = {'url': url, 'body': None, 'requested_at': started, 'received_at': now(), 'http_status': code,
            'headers': headers, 'original': ref, 'status': 'response_retained' if code == 200 and data else 'provider_error_retained'}
        journal(client, bucket, status, {'request_id': request_id, 'source_id': label, 'status': 'complete', 'capture': result})
        return result
    except Exception as exc:
        journal(client, bucket, status, {'request_id': request_id, 'source_id': label, 'status': 'failed', 'error_type': type(exc).__name__})
        raise


def successful(client, bucket, captured):
    if captured['status'] != 'response_retained' or captured['http_status'] != 200:
        raise ValueError('SEC source unavailable; retain current head, never fill or retry')
    return model.original(captured['original'], store.reader(client, bucket))


def collect(client, bucket, request_id, execution_id, deadline, progress, transport=None):
    status = request_key(request_id)
    def save():
        journal(client, bucket, status, {'request_id': request_id, 'execution_id': execution_id, 'status': 'capturing', **progress})
    index = fetch(client, bucket, request_id, 'index', source.INDEX, deadline, transport)
    progress['index'] = index
    save()
    cutoff = model.clock(index['received_at']).date().isoformat()
    selected = source.advertised_archives(successful(client, bucket, index), cutoff, 12)
    progress.update(selection_cutoff=cutoff, selected_archives=selected)
    for url in selected:
        time.sleep(0.2)
        progress['captures'][url] = fetch(client, bucket, request_id, url, url, deadline, transport)
        save()
        if sum(value['original']['bytes'] for value in progress['captures'].values()) > 96 * 1024 * 1024:
            raise ValueError('Whole archive campaign byte bound exceeded')
        # Full compilation validates every row and control after acquisition.
        successful(client, bucket, progress['captures'][url])
    manifest = {'contract': 'sec-settlement-complete-sources.v1', 'request_id': request_id, 'execution_id': execution_id,
        'generated_at': now(), 'index': index, 'selection_cutoff': cutoff, 'selected_archives': selected,
        'captures': progress['captures'], 'source_originals_retained': True, 'snapshot_atomic': False,
        'historical_availability_verified': False, 'security_continuity_verified': False,
        'forecast_qualified': False, 'sizing_qualified': False}
    manifest_ref = protect(client, bucket, model.encoded(manifest))
    progress['source_manifest'] = manifest_ref
    save()
    return {key: progress[key] for key in ('index', 'selection_cutoff', 'selected_archives', 'captures', 'source_manifest', 'predecessor')}


def not_older(packet, previous):
    if previous.get('generated_at') and model.clock(packet['generated_at']) <= model.clock(previous['generated_at']):
        return False
    if previous.get('settlement_date') and packet['settlement_date'] < date.fromisoformat(previous['settlement_date']).isoformat():
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
        deadline = time.monotonic() + min(100, remaining_seconds - 180)
        inputs = collect(client, bucket, request_id, execution_id, deadline, progress, transport)
        inputs.update(contract='sec-ftd-original-inputs.v1', generated_at=now(), request_id=request_id,
            execution_id=execution_id, archive_count=12, collection_policy='latest_twelve_complete_advertised_archives')
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
