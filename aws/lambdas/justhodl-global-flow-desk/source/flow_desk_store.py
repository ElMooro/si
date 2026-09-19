"""Keep every source snapshot and replay the synthesis before publication."""
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
import gzip
import hashlib
import io
import json
import re

import evidence_store
import flow_desk_catalog as catalog
import flow_desk_research as model

PRIVATE = 'audit-private/20260909-originals/global-flow-research/'
LEGACY_HISTORY = 'data/history/global-flow-desk.json'
COMPILERS = (model, catalog, evidence_store)


def now():
    return datetime.now(timezone.utc).isoformat()


def code(exc):
    return str((getattr(exc, 'response', {}) or {}).get('Error', {}).get('Code', ''))


def missing(exc):
    return code(exc) in ('404', 'NoSuchKey')


def conflict(exc):
    return code(exc) in ('409', '412', 'ConditionalRequestConflict', 'PreconditionFailed')


def bounded(stream):
    try:
        raw = stream.read(model.MAX_BYTES + 1)
    finally:
        stream.close()
    if len(raw) > model.MAX_BYTES:
        raise ValueError('Flow research artifact bound')
    return raw


def reader(client, bucket):
    cache, used = {}, 0
    def read(key):
        nonlocal used
        if not isinstance(key, str) or not re.fullmatch(r'data/[A-Za-z0-9_./-]+', key) or '..' in key:
            raise ValueError('Public research path required')
        if key in cache:
            return cache[key]
        raw = bounded(client.get_object(Bucket=bucket, Key=key)['Body'])
        if key.endswith('.gz'):
            raw = bounded(gzip.GzipFile(fileobj=io.BytesIO(raw)))
        if re.search(r'/[a-f0-9]{64}\.(?:json|py|bin\.gz)$', key) and used + len(raw) <= 256 * 1024 * 1024:
            cache[key] = raw
            used += len(raw)
        return raw
    return read


def immutable(client, bucket, key, raw, kind='application/json'):
    if not key.startswith((model.PREFIX, PRIVATE)):
        raise ValueError('Flow retention prefix differs')
    try:
        client.put_object(Bucket=bucket, Key=key, Body=raw, ContentType=kind, IfNoneMatch='*',
                          CacheControl='no-store' if key.startswith(PRIVATE) else 'public, max-age=31536000, immutable')
    except Exception as exc:
        if not conflict(exc):
            raise
    if bounded(client.get_object(Bucket=bucket, Key=key)['Body']) != raw:
        raise ValueError('Retained flow research differs')


def preserve(client, bucket, read):
    key = model.PREFIX + 'migration.json'
    try:
        marker = json.loads(read(key))
    except Exception as exc:
        if not missing(exc):
            raise
        objects = []
        for source in (model.CURRENT, LEGACY_HISTORY):
            raw = read(source)
            packet = json.loads(raw)
            if source == model.CURRENT and packet.get('contract') == model.CONTRACT:
                raise ValueError('Previous synthesis preservation missing')
            sha = hashlib.sha256(raw).hexdigest()
            immutable(client, bucket, PRIVATE + sha + '.bin', raw, 'application/octet-stream')
            objects.append({'source': source, 'sha256': sha, 'bytes': len(raw), 'protected_backup': True})
        marker = {'contract': 'flow-desk-legacy.v1', 'objects': objects,
                  'status': 'Complete preceding packet and history preserved; legacy scores do not acquire research authority.'}
        immutable(client, bucket, key, model.encoded(marker))
    if marker.get('contract') != 'flow-desk-legacy.v1' or len(marker.get('objects', [])) != 2:
        raise ValueError('Synthesis preservation marker differs')
    return marker


def capture_contexts(client, bucket):
    def capture(key):
        try:
            raw = bounded(client.get_object(Bucket=bucket, Key=key)['Body'])
        except Exception as exc:
            return key, {'status': 'unavailable', 'failure_class': code(exc) or type(exc).__name__}
        status = 'retained'
        try:
            model.decode(raw)
        except (ValueError, UnicodeError):
            status = 'unparseable'
        sha = hashlib.sha256(raw).hexdigest()
        target = model.PREFIX + 'contexts/' + sha + '.json'
        immutable(client, bucket, target, raw, 'application/json' if status == 'retained' else 'application/octet-stream')
        return key, {'status': status, 'acquired_at': now(),
                     'artifact': {'key': target, 'sha256': sha, 'bytes': len(raw)}}
    with ThreadPoolExecutor(max_workers=6) as pool:
        return dict(pool.map(capture, catalog.CONTEXT_KEYS))


def source_ref(packet, prefix):
    key = packet['replay']['manifest_key']
    if not re.fullmatch(re.escape(prefix) + r'runs/[a-f0-9]{64}\.json', key):
        raise ValueError('Source run identity differs')
    return {'key': key, 'sha256': key.rsplit('/', 1)[-1][:-5]}


def current_matches(packet, ref, prefix, read):
    manifest = model.load(ref, read, prefix + 'runs/')
    expected = model.load(manifest['output'], read, prefix + 'outputs/')
    if ({k: v for k, v in packet.items() if k != 'replay'} != expected
            or packet['replay']['output_sha256'] != manifest['output']['sha256']):
        raise ValueError('Current source differs from immutable output')


def publish(client, bucket, packet):
    for _ in range(5):
        obj = client.get_object(Bucket=bucket, Key=model.CURRENT)
        old = json.loads(bounded(obj['Body']))
        if old.get('contract') == model.CONTRACT:
            if (model.clock(old['generated_at']) > model.clock(packet['generated_at'])
                    or model.clock(old['source_generated_at']) > model.clock(packet['source_generated_at'])):
                return False
            if old['generated_at'] == packet['generated_at'] and old != packet:
                raise ValueError('Same-clock synthesis conflict')
        try:
            client.put_object(Bucket=bucket, Key=model.CURRENT, Body=model.encoded(packet),
                              ContentType='application/json', CacheControl='no-store', IfMatch=obj['ETag'])
            return True
        except Exception as exc:
            if not conflict(exc):
                raise
    raise RuntimeError('Synthesis publication contention')


def replay(manifest, read):
    if (manifest.get('contract') != 'flow-desk-replay.v1'
            or set(manifest.get('compilers', {})) != {v.__name__ for v in COMPILERS}):
        raise ValueError('Synthesis replay contract differs')
    for module in COMPILERS:
        raw = Path(module.__file__).read_bytes()
        sha = hashlib.sha256(raw).hexdigest()
        ref = manifest['compilers'][module.__name__]
        if ref != {'key': model.PREFIX + 'compilers/' + sha + '.py', 'sha256': sha} or read(ref['key']) != raw:
            raise ValueError('Reviewed synthesis compiler differs')
    inputs = model.load(manifest['input'], read, model.PREFIX + 'inputs/')
    output, histories = model.build(inputs, read, manifest['generated_at'])
    if (output != model.load(manifest['output'], read, model.PREFIX + 'outputs/')
            or model.digest(output) != manifest['output_sha256']):
        raise ValueError('Synthesis output replay differs')
    for key, raw in histories.items():
        if read(key) != raw:
            raise ValueError('Synthesis complete history differs')
    return output


def run(client, bucket):
    read = reader(client, bucket)
    legacy = preserve(client, bucket, read)
    etf = json.loads(read('data/etf-true-flows.json'))
    etf_ref = source_ref(etf, model.ETF)
    current_matches(etf, etf_ref, model.ETF, read)
    tic_ref, tic_status = None, 'unavailable'
    try:
        tic = json.loads(read('data/tic-flows.json'))
        tic_ref = source_ref(tic, model.TIC)
        current_matches(tic, tic_ref, model.TIC, read)
        tic_status = 'retained_verified_snapshot'
    except Exception as exc:
        # A corrupt immutable source is not silently converted to missing context.
        if not missing(exc):
            raise
    contexts = capture_contexts(client, bucket)
    stamp = now()
    inputs = {'contract': 'flow-desk-inputs.v1', 'etf_manifest': etf_ref, 'tic_manifest': tic_ref,
              'tic_status': tic_status, 'contexts': contexts, 'legacy': legacy}
    body = model.encoded(inputs); sha = model.digest(inputs)
    key = model.PREFIX + 'inputs/' + sha + '.json'
    immutable(client, bucket, key, body)
    refs = {'input': {'key': key, 'sha256': sha, 'bytes': len(body)}}
    compilers = {}
    for module in COMPILERS:
        body = Path(module.__file__).read_bytes(); sha = hashlib.sha256(body).hexdigest()
        key = model.PREFIX + 'compilers/' + sha + '.py'
        immutable(client, bucket, key, body, 'text/x-python')
        compilers[module.__name__] = {'key': key, 'sha256': sha}
    try:
        output, histories = model.build(inputs, read, stamp)
    except Exception as exc:
        failed = {'contract': 'flow-desk-failed-attempt.v1', 'input': refs['input'],
                  'generated_at': stamp, 'failure_class': type(exc).__name__, 'compilers': compilers,
                  'published': False}
        key = model.PREFIX + 'attempts/' + model.digest(failed) + '.json'
        immutable(client, bucket, key, model.encoded(failed))
        print('[flow-research] ' + json.dumps({'failed_attempt': key, 'failure_class': type(exc).__name__}))
        raise
    count = len(histories)
    with ThreadPoolExecutor(max_workers=8) as pool:
        futures = [pool.submit(immutable, client, bucket, key, raw) for key, raw in histories.items()]
        for future in as_completed(futures):
            future.result()
    del histories
    body = model.encoded(output); sha = model.digest(output)
    key = model.PREFIX + 'outputs/' + sha + '.json'
    immutable(client, bucket, key, body)
    refs['output'] = {'key': key, 'sha256': sha, 'bytes': len(body)}
    manifest = {'contract': 'flow-desk-replay.v1', 'generated_at': stamp,
                'compilers': compilers, **refs, 'output_sha256': sha}
    key = model.PREFIX + 'runs/' + model.digest(manifest) + '.json'
    immutable(client, bucket, key, model.encoded(manifest))
    if replay(manifest, read) != output:
        raise ValueError('Pre-publication synthesis replay differs')
    proof = {'manifest_key': key, 'output_sha256': sha}
    published = publish(client, bucket, {**output, 'replay': proof})
    return {'published': published, 'generated_at': stamp, 'replay': proof, 'quality': output['quality'],
            'history_shards': count, 'signals_emitted': 0, 'paid_ai_calls': 0,
            'notifications_sent': 0, 'private_account_reads': 0, 'portfolio_writes': 0}
