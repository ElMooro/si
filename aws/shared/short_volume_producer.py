"""Bounded native FINRA acquisition, original replay and conditional publication.

Each attempt rechecks all 61 published files, retaining revisions as distinct
originals. No paid source, credential read, account, notification or order path.
Failed request identities require diagnosis; they cannot silently recollect.
"""
from datetime import datetime, timezone, date, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urlsplit, parse_qs
import gc, re, time, urllib.request, urllib.error
import short_volume_research_model as model
import short_volume_research_store as store
import short_volume_source_index as index


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
    raw = model.encoded(value)
    if len(raw) > model.MAX:
        raise ValueError('Request journal exceeds bound')
    client.put_object(Bucket=bucket, Key=key, Body=raw, ContentType='application/json', CacheControl='no-store',
                      **({'IfNoneMatch': '*'} if claim else {}))
    if raw_read(client, bucket, key) != raw:
        raise ValueError('Journal readback differs')


def protect(client, bucket, raw):
    if not isinstance(raw, bytes) or len(raw) > model.MAX:
        raise ValueError('Whole bounded original required')
    ref = {'key': model.PRIVATE + model.sha(raw) + '.bin', 'sha256': model.sha(raw), 'bytes': len(raw)}
    try:
        client.put_object(Bucket=bucket, Key=ref['key'], Body=raw, ContentType='application/octet-stream',
                          CacheControl='no-store', IfNoneMatch='*')
    except Exception as exc:
        if not store.conflict(exc):
            raise
    if raw_read(client, bucket, ref['key']) != raw:
        raise ValueError('Original readback differs')
    return ref


def reviewed(url):
    if index.FILE.fullmatch(url):
        return url
    parsed = urlsplit(url)
    if parsed.scheme != 'https' or parsed.netloc != 'www.finra.org' or parsed.path != index.PATH or parsed.fragment:
        raise ValueError('Unreviewed public-source URL')
    if not parsed.query:
        return url
    query = parse_qs(parsed.query, keep_blank_values=True, strict_parsing=True)
    if set(query) != {index.MONTH, index.YEAR} or any(len(v) != 1 for v in query.values()):
        raise ValueError('Reviewed source filter required')
    if query[index.MONTH][0] not in [f'{v:02d}' for v in range(1, 13)] or not re.fullmatch(r'\d{1,3}', query[index.YEAR][0]):
        raise ValueError('Invalid public-source filter')
    return url


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *args, **kwargs):
        raise ValueError('Unreviewed provider redirect refused')


def fetch(client, bucket, request_id, label, url, deadline, transport=None):
    reviewed(url)
    status = request_key(model.sha((request_id + ':' + label).encode()))
    journal(client, bucket, status, {'request_id': request_id, 'source_id': label, 'status': 'claimed', 'url': url}, True)
    started = now()
    try:
        if time.monotonic() >= deadline:
            raise TimeoutError('Source campaign deadline')
        request = urllib.request.Request(url, headers={'User-Agent': 'JustHodl-ShortVolumeResearch/2.0',
                                                       'Accept': 'text/plain' if index.FILE.fullmatch(url) else 'text/html'})
        try:
            response = (transport or urllib.request.build_opener(NoRedirect()).open)(request, timeout=max(1, min(20, deadline - time.monotonic())))
        except urllib.error.HTTPError as exc:
            response = exc
        code = response.status
        headers = {k.lower(): v for k, v in response.headers.items()
                   if k.lower() in ('content-type', 'content-length', 'last-modified', 'etag', 'date')}
        raw = store.bounded(response)
        ref = protect(client, bucket, raw)
        result = {'url': url, 'requested_at': started, 'received_at': now(), 'http_status': code,
                  'headers': headers, 'original': ref,
                  'status': 'response_retained' if code == 200 and raw else 'empty_http_response' if not raw else 'provider_error_retained'}
        journal(client, bucket, status, {'request_id': request_id, 'source_id': label, 'status': 'complete', 'capture': result})
        return result
    except Exception as exc:
        journal(client, bucket, status, {'request_id': request_id, 'source_id': label, 'status': 'failed',
                                        'url': url, 'error_type': type(exc).__name__})
        raise


def successful(client, bucket, captured):
    if captured['status'] != 'response_retained' or captured['http_status'] != 200:
        raise ValueError('Listed source unavailable; no retry or fabricated zero')
    return model.original(captured['original'], store.reader(client, bucket))


def not_older(packet, previous):
    if previous.get('generated_at') and model.clock(packet['generated_at']) <= model.clock(previous['generated_at']):
        return False
    if previous.get('data_date') and packet['data_date'] < model.source.day(previous['data_date']):
        return False
    return True


def run(client, bucket, request_id, execution_id, remaining_seconds=300, transport=None):
    if not isinstance(execution_id, str) or not execution_id:
        raise ValueError('Actual execution identity required')
    if remaining_seconds < 230:
        raise ValueError('Insufficient acquisition and replay budget')
    status = request_key(request_id)
    try:
        existing = model.strict(raw_read(client, bucket, status))
    except Exception as exc:
        if not missing(exc):
            raise
        existing = None
    if existing:
        if existing.get('status') != 'complete':
            raise ValueError('Request already attempted; inspect retained status instead of recollecting')
        return existing['result']
    journal(client, bucket, status, {'request_id': request_id, 'execution_id': execution_id,
                                   'status': 'claimed', 'started_at': now()}, True)
    progress = {'captures': {}, 'errors': {}}
    try:
        head = client.get_object(Bucket=bucket, Key=model.CURRENT)
        previous_raw = store.bounded(head['Body'])
        previous = model.strict(previous_raw)
        etag = head['ETag']
        progress['predecessor'] = protect(client, bucket, previous_raw)
        deadline = time.monotonic() + min(135, remaining_seconds - 150)
        cutoff = datetime.now(timezone.utc).date()
        seed = fetch(client, bucket, request_id, 'index:discovery', index.URL, deadline, transport)
        progress['captures']['index:discovery'] = seed
        discovery = index.parse(successful(client, bucket, seed), cutoff)
        plan = index.month_requests(cutoff, discovery)
        indexes = []
        for spec in plan:
            label = 'index:' + spec['period']
            captured = fetch(client, bucket, request_id, label, spec['url'], deadline, transport)
            progress['captures'][label] = captured
            indexes.append(index.parse(successful(client, bucket, captured), cutoff, spec['period']))
            journal(client, bucket, status, {'request_id': request_id, 'execution_id': execution_id, 'status': 'capturing', **progress})
        selected = index.selected_files(indexes, cutoff)
        if date.fromisoformat(selected[-1]['observation_date']) < cutoff - timedelta(days=7):
            raise ValueError('Latest listed source is unexpectedly old')
        progress.update(month_plan=plan, selected_files=selected, selection_cutoff=cutoff.isoformat())
        def task(row):
            captured = fetch(client, bucket, request_id, 'daily:' + row['observation_date'], row['url'], deadline, transport)
            successful(client, bucket, captured)
            return captured
        with ThreadPoolExecutor(max_workers=3) as pool:
            jobs = {pool.submit(task, row): 'daily:' + row['observation_date'] for row in selected}
            for future in as_completed(jobs):
                label = jobs[future]
                try:
                    progress['captures'][label] = future.result()
                except Exception as exc:
                    progress['errors'][label] = type(exc).__name__
                journal(client, bucket, status, {'request_id': request_id, 'execution_id': execution_id, 'status': 'capturing', **progress})
        if progress['errors']:
            raise ValueError('One or more listed sources failed; current head retained')
        inputs = {k: progress[k] for k in ('captures', 'month_plan', 'selected_files', 'selection_cutoff', 'predecessor')}
        inputs.update(contract='short-volume-original-inputs.v1', generated_at=now(), request_id=request_id,
                      execution_id=execution_id, collection_policy='recheck_all_61_listed_files_for_revisions')
        compiled = model.compile_output(inputs, store.reader(client, bucket))
        ref = store.retain(client, bucket, inputs, compiled)
        expected = model.digest(compiled['packet'])
        del compiled
        gc.collect()
        replayed = store.replay(ref, store.reader(client, bucket))
        packet = replayed['packet']
        if model.digest(packet) != expected:
            raise ValueError('Retained reconstruction differs')
        if not not_older(packet, previous):
            result = {'published': False, 'reason': 'observation_or_publication_rollback', 'replay': ref}
        else:
            published = {**packet, 'replay': ref}
            try:
                client.put_object(Bucket=bucket, Key=model.CURRENT, Body=model.encoded(published),
                                  ContentType='application/json', CacheControl='no-store', IfMatch=etag)
            except Exception as exc:
                if not store.conflict(exc):
                    raise
                result = {'published': False, 'reason': 'concurrent_publication', 'replay': ref}
            else:
                if raw_read(client, bucket, model.CURRENT) != model.encoded(published):
                    raise ValueError('Native publication readback differs')
                result = {'published': True, 'generated_at': packet['generated_at'], 'counts': packet['counts'], 'replay': ref}
        journal(client, bucket, status, {'request_id': request_id, 'execution_id': execution_id, 'status': 'complete', 'result': result})
        return result
    except Exception as exc:
        journal(client, bucket, status, {'request_id': request_id, 'execution_id': execution_id, 'status': 'failed',
                                       'error_type': type(exc).__name__, **progress})
        raise
