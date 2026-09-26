"""Retain and replay complete derived public sidecars before publishing inventory.

This is an inventory of engine reports, not original-provider or model validation.
Acquisition has one durable attempt per source and scheduled six-hour slot.
"""
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path
import hashlib, json, re, sys, time, urllib.request, urllib.error
import signal_board_candidate as candidate
import verify_signal_board_inventory as independent

ROOT = Path(__file__).resolve().parent
CURRENT = 'data/signal-board.json'
PREFIX = 'data/signal-board-research/'
PRIVATE = 'audit-private/20260909-originals/signal-board-research/'
MAX = 64 * 1024 * 1024
QUALIFIED = {'signal_board_candidate.py': '5fdb0968d19c5c8c9332d887a79a6ae921aef858932ec4d9ebfe7c7a438aa3c6',
             'verify_signal_board_inventory.py': 'd7d5353be93fbb2abf841544d172f145be36107a79e66150f3deb669ffff41df'}
QUALIFICATION = 'b8a507a655bef246f08384504a9e139ed8e254a793ec42dbf7affeecef865054'
COMPILERS = ('signal_board_candidate', 'verify_signal_board_inventory', 'board_store', 'lambda_function')
REGISTRY_SHA = '92ea428ef964bf9df9d07a5eb87e1e7bd66a6db60d4d9a915f2cd7f89730b07a'
encoded = candidate.encoded
sha = candidate.sha
now = lambda: datetime.now(timezone.utc).isoformat()
conflict = lambda e: str(getattr(e, 'response', {}).get('Error', {}).get('Code')) in ('409', '412', 'ConditionalRequestConflict', 'PreconditionFailed')


def bounded(stream, limit=MAX):
    try: raw = stream.read(limit + 1)
    finally: stream.close()
    if len(raw) > limit: raise ValueError('Whole response exceeds retention limit')
    return raw


def qualified():
    for name, digest in QUALIFIED.items():
        if sha((ROOT/name).read_bytes()) != digest: raise ValueError('Accepted compiler differs: '+name)
    raw = (ROOT/'board_registry.json').read_bytes()
    if sha(raw) != REGISTRY_SHA: raise ValueError('Qualified registry differs')
    rows = json.loads(raw)
    return candidate.registry(rows)


def allowed(key):
    return isinstance(key, str) and (key == CURRENT or bool(re.fullmatch(
        re.escape(PREFIX)+r'(?:runs|inputs|views|proofs|compilers|originals)/[a-f0-9]{64}\.(?:json|py|bin)', key)))


def reader(client, bucket):
    def read(key):
        if not allowed(key): raise ValueError('Unreviewed artifact path')
        return bounded(client.get_object(Bucket=bucket, Key=key)['Body'])
    return read


def retain(client, bucket, raw, kind, extension='json', private=False):
    if not isinstance(raw, bytes) or len(raw) > MAX: raise ValueError('Whole bounded bytes required')
    if kind not in ('runs', 'inputs', 'views', 'proofs', 'compilers', 'originals') or extension not in ('json', 'py', 'bin'):
        raise ValueError('Unreviewed artifact type')
    key = PRIVATE+sha(raw)+'.bin' if private else PREFIX+kind+'/'+sha(raw)+'.'+extension
    try:
        client.put_object(Bucket=bucket, Key=key, Body=raw, IfNoneMatch='*',
                          ContentType='application/octet-stream' if private or extension == 'bin' else 'text/plain' if extension == 'py' else 'application/json',
                          CacheControl='no-store' if private else 'public, max-age=31536000, immutable')
    except Exception as exc:
        if not conflict(exc): raise
    if bounded(client.get_object(Bucket=bucket, Key=key)['Body']) != raw: raise ValueError('Retention readback differs')
    return {'key': key, 'sha256': sha(raw), 'bytes': len(raw)}


def checked(ref, kind, read, extension='json'):
    candidate.reference(ref)
    if ref['key'] != PREFIX+kind+'/'+ref['sha256']+'.'+extension: raise ValueError('Immutable coordinates required')
    raw = read(ref['key'])
    if len(raw) != ref['bytes'] or sha(raw) != ref['sha256']: raise ValueError('Whole artifact differs')
    return raw if extension != 'json' else json.loads(raw)


def binding(packet, read):
    key = (packet.get('replay') or {}).get('manifest_key', '')
    if not re.fullmatch(re.escape(PREFIX)+r'runs/[a-f0-9]{64}\.json', key): raise ValueError('Native run required')
    raw = read(key); run = json.loads(raw)
    if key != PREFIX+'runs/'+sha(raw)+'.json' or run.get('contract') != 'signal-board-replay.v1': raise ValueError('Run binding differs')
    if (run['qualification'] != QUALIFICATION or packet['replay'].get('view_sha256') != run['view']['sha256'] or
            encoded(checked(run['view'], 'views', read)) != encoded({k: v for k, v in packet.items() if k != 'replay'}) or run['generated_at'] != packet['generated_at']):
        raise ValueError('Whole inventory binding differs')
    return run


def replay(packet, read):
    rows = qualified(); run = binding(packet, read)
    if set(run['compilers']) != set(COMPILERS): raise ValueError('Whole compiler closure required')
    for name in COMPILERS:
        if checked(run['compilers'][name], 'compilers', read, 'py') != (ROOT/(name+'.py')).read_bytes():
            raise ValueError('Reviewed compiler bytes differ')
    inputs = checked(run['input'], 'inputs', read)
    if inputs['registry'] != rows or inputs['generated_at'] != packet['generated_at']: raise ValueError('Complete native registry required')
    source = lambda ref: checked(ref, 'originals', read, 'bin')
    view = candidate.build(rows, inputs['captures'], source, inputs['generated_at'])
    proof = independent.verify(view, rows, inputs['captures'], source)
    if encoded(view) != encoded({k: v for k, v in packet.items() if k != 'replay'}) or encoded(proof) != encoded(checked(run['proof'], 'proofs', read)):
        raise ValueError('Complete independent reconstruction differs')
    return proof


def assemble(client, bucket, inputs):
    rows = qualified()
    if inputs['registry'] != rows: raise ValueError('Reviewed registry differs')
    read = reader(client, bucket); source = lambda ref: checked(ref, 'originals', read, 'bin')
    view = candidate.build(rows, inputs['captures'], source, inputs['generated_at'])
    proof = independent.verify(view, rows, inputs['captures'], source)
    put = lambda value, kind: retain(client, bucket, encoded(value), kind)
    run = {'contract': 'signal-board-replay.v1', 'generated_at': inputs['generated_at'], 'qualification': QUALIFICATION,
           'input': put(inputs, 'inputs'), 'view': put(view, 'views'), 'proof': put(proof, 'proofs'),
           'compilers': {name: retain(client, bucket, (ROOT/(name+'.py')).read_bytes(), 'compilers', 'py') for name in COMPILERS}}
    ref = put(run, 'runs'); packet = {**view, 'replay': {'manifest_key': ref['key'], 'view_sha256': run['view']['sha256']}}
    if replay(packet, reader(client, bucket)) != proof: raise ValueError('Retained source replay differs')
    return packet


def publish(client, bucket, packet):
    # Independently verify even if a caller bypasses assemble().
    replay(packet, reader(client, bucket)); stamp = candidate.stamp(packet['generated_at'])
    for _ in range(4):
        obj = client.get_object(Bucket=bucket, Key=CURRENT); raw = bounded(obj['Body'])
        try: old = json.loads(raw)
        except (ValueError, UnicodeError): old = {}
        if not isinstance(old, dict): old = {}
        prior = candidate.stamp(old.get('generated_at'))
        if prior and prior > stamp: return False
        if prior == stamp:
            if encoded(old) != encoded(packet): raise ValueError('Conflicting same-clock publication')
            return True
        retain(client, bucket, raw, 'originals', private=True)
        try:
            client.put_object(Bucket=bucket, Key=CURRENT, Body=encoded(packet), ContentType='application/json', CacheControl='no-store', IfMatch=obj['ETag'])
        except Exception as exc:
            if not conflict(exc): raise
            continue
        actual = json.loads(reader(client, bucket)(CURRENT))
        if encoded(actual) != encoded(packet):
            next_stamp = candidate.stamp(actual.get('generated_at'))
            if next_stamp is None or next_stamp <= stamp: raise ValueError('Publication readback differs')
        return True
    raise RuntimeError('Publication conflict ceiling; complete immutable run retained')


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl): return None


def acquire(key, timeout=20, transport=None):
    keys = {r['source_key'] for r in qualified()} - candidate.PRIVATE
    if key not in keys: raise ValueError('Unreviewed public sidecar')
    # Only /data/* has a zone route on Pages. Preserve the exact registered
    # object key for other public namespaces through the existing data Worker.
    base = 'https://justhodl.ai/' if key.startswith('data/') else 'https://justhodl-data-proxy.raafouis.workers.dev/'
    url = base+key+'?exact=1&nogen=1'
    req = urllib.request.Request(url, headers={'User-Agent': 'justhodl-verify-release/1.0', 'Cache-Control': 'no-cache'})
    opened = transport or urllib.request.build_opener(NoRedirect()).open
    try: response = opened(req, timeout=timeout)
    except urllib.error.HTTPError as exc: response = exc
    status = response.status
    headers = {k.lower(): v for k, v in response.headers.items() if k.lower() in ('content-type', 'content-length', 'cache-control', 'date', 'last-modified', 'etag', 'x-jh-artifact-key')}
    raw = bounded(response)
    if 'content-length' in headers and int(headers['content-length']) != len(raw): raise ValueError('Incomplete HTTP response')
    if status == 200 and headers.get('x-jh-artifact-key') != key: raise ValueError('Exact public artifact identity differs')
    return raw, status, headers


def slot(event, instant=None):
    # Standard EventBridge time is stable across retries; empty payloads share the same six-hour slot.
    stamp = candidate.stamp(event['time']) if 'time' in event else (instant or datetime.now(timezone.utc))
    if stamp is None: raise ValueError('Valid scheduled timestamp required')
    return stamp.replace(hour=stamp.hour//6*6, minute=0, second=0, microsecond=0).isoformat()


def run(client, bucket, request_id, acquire_fn=acquire):
    rows = qualified()
    if not isinstance(request_id, str) or not 1 <= len(request_id) <= 200: raise ValueError('Stable native request required')
    root = PRIVATE+'requests/'+sha(('native:'+request_id).encode())
    progress = {'status': 'claimed', 'request_id': request_id, 'started_at': now()}
    def write(key, value, claim=False):
        raw = encoded(value)
        client.put_object(Bucket=bucket, Key=key, Body=raw, ContentType='application/json', CacheControl='no-store', **({'IfNoneMatch': '*'} if claim else {}))
        if bounded(client.get_object(Bucket=bucket, Key=key)['Body']) != raw: raise ValueError('Durable journal differs')
    write(root+'.json', progress, True)
    try:
        # Capture the complete prior publication without reading its private donors.
        previous = retain(client, bucket, bounded(client.get_object(Bucket=bucket, Key=CURRENT)['Body']), 'originals', private=True)
        deadline = time.monotonic()+240
        def capture(key):
            if key in candidate.PRIVATE: return {'source_key': key, 'status': 'excluded_private_account_input', 'requested': False}
            attempt_key = root+'/'+sha(key.encode())+'.json'
            entry = {'source_key': key, 'status': 'transport_or_size_unavailable', 'requested': False}
            remaining = deadline-time.monotonic()
            if remaining <= 1:
                entry['reason'] = 'acquisition_budget_exhausted'; write(attempt_key, entry, True); return entry
            entry.update(requested=True, requested_at=now())
            write(attempt_key, {**entry, 'status': 'attempt_recorded'}, True)
            try: raw, status, headers = acquire_fn(key, timeout=min(20, remaining))
            except Exception as exc:
                entry.update(error_type=type(exc).__name__, received_at=now())
            else:
                entry.update(status='public_sidecar_retained' if status == 200 else 'whole_http_error_retained', http_status=status,
                             headers=headers, received_at=now(), original=retain(client, bucket, raw, 'originals', 'bin'))
            write(attempt_key, entry); return entry
        keys = sorted({r['source_key'] for r in rows})
        with ThreadPoolExecutor(max_workers=6) as pool: captures = dict(zip(keys, pool.map(capture, keys)))
        inputs = {'contract': 'signal-board-inputs.v1', 'generated_at': now(), 'registry': rows, 'captures': captures,
                  'predecessor': previous, 'acquisition': {'public_attempts': sum(x.get('requested') is True for x in captures.values()),
                  'max_attempts_per_source': 1, 'request_budget_seconds': 240, 'max_concurrency': 6,
                  'source_scope': 'anonymous derived public sidecars; original providers not read', 'snapshot_atomic': False}}
        packet = assemble(client, bucket, inputs); published = publish(client, bucket, packet)
        result = {'published': published, 'generated_at': packet['generated_at'], 'replay': packet['replay'],
                  'source_status_counts': packet['source_status_counts'], 'public_attempts': inputs['acquisition']['public_attempts'],
                  'private_account_reads': 0, 'provider_requests': 0, 'paid_api_calls': 0, 'notifications_sent': 0,
                  **dict.fromkeys(candidate.PERMISSIONS, False)}
        write(root+'.json', {**progress, 'status': 'complete', 'result': result}); return result
    except Exception as exc:
        write(root+'.json', {**progress, 'status': 'failed', 'error_type': type(exc).__name__}); raise
