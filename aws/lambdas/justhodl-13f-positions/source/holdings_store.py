"""Retained-source holdings research publication, isolated from legacy writers.

The explicit research action does not execute the legacy collector, notifications,
price services, alias cache or score emitters. Consumer migration is separate.
"""
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path
import gzip
import hashlib
import io
import json
import re
import sys

import holdings_native as model

PRIVATE = 'audit-private/20260909-originals/holdings-research/'
LEGACY_KEYS = ('data/13f-positions.json', 'data/13f-flows-by-ticker.json', 'data/13f-by-ticker.json',
               'data/13f-desk.json', 'data/13f-state/first-seen.json', 'data/13f-price-anchors.json',
               'data/13f-cusip-map.json', 'data/capital-flow.json', 'data/capital-flow-history.json')


def error_code(exc):
    return str((getattr(exc, 'response', {}) or {}).get('Error', {}).get('Code', ''))


def missing(exc):
    return error_code(exc) in ('404', 'NoSuchKey')


def conflict(exc):
    return error_code(exc) in ('409', '412', 'ConditionalRequestConflict', 'PreconditionFailed')


def bounded(stream):
    try:
        raw = stream.read(model.MAX_BYTES + 1)
    finally:
        stream.close()
    if len(raw) > model.MAX_BYTES:
        raise ValueError('Whole holdings artifact exceeds explicit bound')
    return raw


def reader(client, bucket):
    cache, used = {}, 0
    def read(key):
        nonlocal used
        if not isinstance(key, str) or not re.fullmatch(r'data/[A-Za-z0-9_./-]+', key) or '..' in key:
            raise ValueError('Public research artifact path required')
        if key in cache:
            return cache[key]
        raw = bounded(client.get_object(Bucket=bucket, Key=key)['Body'])
        if key.endswith('.gz'):
            raw = bounded(gzip.GzipFile(fileobj=io.BytesIO(raw)))
        if re.search(r'/[a-f0-9]{64}\.(?:json|py|bin\.gz)$', key) and used + len(raw) <= 100 * 1024 * 1024:
            cache[key] = raw; used += len(raw)
        return raw
    return read


def immutable(client, bucket, key, raw, content_type='application/json'):
    if not key.startswith((model.PREFIX, PRIVATE)) or not 0 < len(raw) <= model.MAX_BYTES:
        raise ValueError('Holdings retention path or size differs')
    try:
        client.put_object(Bucket=bucket, Key=key, Body=raw, ContentType=content_type, IfNoneMatch='*',
                          CacheControl='no-store' if key.startswith(PRIVATE) else 'public, max-age=31536000, immutable')
    except Exception as exc:
        if not conflict(exc):
            raise
    if bounded(client.get_object(Bucket=bucket, Key=key)['Body']) != raw:
        raise ValueError('Retained holdings bytes differ')


def reference(key, raw):
    return {'key': key, 'sha256': hashlib.sha256(raw).hexdigest(), 'bytes': len(raw)}


def verified(ref, read, subdir):
    sha = ref.get('sha256', '')
    if not re.fullmatch('[a-f0-9]{64}', sha) or ref.get('key') != model.PREFIX + subdir + '/' + sha + '.json':
        raise ValueError('Holdings immutable identity differs')
    raw = read(ref['key'])
    if hashlib.sha256(raw).hexdigest() != sha or ref.get('bytes', len(raw)) != len(raw):
        raise ValueError('Holdings immutable bytes differ')
    return model.decode(raw)


def preserve(client, bucket):
    """Snapshot whole preceding public products; do not mutate legacy sources."""
    objects = []
    for key in LEGACY_KEYS:
        try:
            raw = bounded(client.get_object(Bucket=bucket, Key=key)['Body'])
        except Exception as exc:
            if not missing(exc):
                raise
            objects.append({'source': key, 'status': 'absent_at_snapshot'})
            continue
        sha = hashlib.sha256(raw).hexdigest()
        immutable(client, bucket, PRIVATE + sha + '.bin', raw, 'application/octet-stream')
        objects.append({'source': key, 'sha256': sha, 'bytes': len(raw), 'status': 'whole_private_snapshot'})
    marker = {'contract': 'holdings-legacy-snapshot.v1', 'objects': objects,
              'scope': 'Whole listed products retained. Legacy caches and other producers are unchanged and unqualified.'}
    raw = model.encoded(marker); key = model.PREFIX + 'legacy/' + model.digest(marker) + '.json'
    immutable(client, bucket, key, raw)
    return reference(key, raw)


def publish(client, bucket, packet):
    for _ in range(5):
        try:
            previous = client.get_object(Bucket=bucket, Key=model.CURRENT)
            old = model.decode(bounded(previous['Body'])); condition = {'IfMatch': previous['ETag']}
            if old.get('contract') != model.CONTRACT:
                raise ValueError('Unexpected existing holdings research contract')
            if model.clock(old['generated_at']) > model.clock(packet['generated_at']) or model.clock(old['source_generated_at']) > model.clock(packet['source_generated_at']):
                return False
            if old['generated_at'] == packet['generated_at'] and old != packet:
                raise ValueError('Same-clock holdings publication conflict')
        except Exception as exc:
            if not missing(exc):
                raise
            condition = {'IfNoneMatch': '*'}
        try:
            client.put_object(Bucket=bucket, Key=model.CURRENT, Body=model.encoded(packet),
                              ContentType='application/json', CacheControl='no-store', **condition)
            return True
        except Exception as exc:
            if not conflict(exc):
                raise
    raise RuntimeError('Holdings publication contention')


def replay(manifest, read):
    if manifest.get('contract') != 'holdings-native-replay.v1':
        raise ValueError('Holdings replay contract differs')
    modules = (model, sys.modules[__name__])
    if set(manifest.get('compilers', {})) != {v.__name__ for v in modules}:
        raise ValueError('Reviewed holdings compiler set differs')
    for module in modules:
        code = Path(module.__file__).read_bytes(); sha = hashlib.sha256(code).hexdigest()
        ref = manifest['compilers'][module.__name__]
        if ref != {'key': model.PREFIX + 'compilers/' + sha + '.py', 'sha256': sha} or read(ref['key']) != code:
            raise ValueError('Reviewed holdings compiler differs')
    inputs = verified(manifest['input'], read, 'inputs')
    probe = verified(inputs['probe'], read, 'probes')
    legacy = verified(inputs['legacy'], read, 'legacy')
    if legacy.get('contract') != 'holdings-legacy-snapshot.v1' or {v['source'] for v in legacy.get('objects', [])} != set(LEGACY_KEYS):
        raise ValueError('Whole preceding products not accounted for')
    output, artifacts = model.build(probe, read, manifest['generated_at'])
    output.update(legacy_snapshot=inputs['legacy'], acquisition_mode='retained_source_snapshot',
                  refresh_status='Source audit snapshot. Scheduled collector and legacy consumer migration remain in progress.')
    if output != verified(manifest['output'], read, 'outputs') or model.digest(output) != manifest['output_sha256']:
        raise ValueError('Holdings output replay differs')
    for key, raw in artifacts.items():
        if read(key) != raw:
            raise ValueError('Complete native filing or fund replay differs')
    return output


def run(client, bucket, probe_ref):
    read = reader(client, bucket)
    probe = verified(probe_ref, read, 'probes')
    legacy = preserve(client, bucket)
    stamp = datetime.now(timezone.utc).isoformat()
    inputs = {'contract': 'holdings-native-inputs.v1', 'probe': probe_ref, 'legacy': legacy}
    body = model.encoded(inputs); key = model.PREFIX + 'inputs/' + model.digest(inputs) + '.json'
    immutable(client, bucket, key, body); input_ref = reference(key, body)
    compilers = {}
    for module in (model, sys.modules[__name__]):
        code = Path(module.__file__).read_bytes(); sha = hashlib.sha256(code).hexdigest()
        ref = {'key': model.PREFIX + 'compilers/' + sha + '.py', 'sha256': sha}
        immutable(client, bucket, ref['key'], code, 'text/x-python')
        compilers[module.__name__] = ref
    try:
        output, artifacts = model.build(probe, read, stamp)
        output.update(legacy_snapshot=legacy, acquisition_mode='retained_source_snapshot',
                      refresh_status='Source audit snapshot. Scheduled collector and legacy consumer migration remain in progress.')
        count = len(artifacts)
        with ThreadPoolExecutor(max_workers=6) as pool:
            list(pool.map(lambda item: immutable(client, bucket, *item), artifacts.items()))
        del artifacts
        body = model.encoded(output); key = model.PREFIX + 'outputs/' + model.digest(output) + '.json'
        immutable(client, bucket, key, body)
        manifest = {'contract': 'holdings-native-replay.v1', 'generated_at': stamp, 'input': input_ref,
                    'compilers': compilers, 'output': reference(key, body), 'output_sha256': model.digest(output)}
        if replay(manifest, read) != output:
            raise ValueError('Prepublication holdings replay differs')
        key = model.PREFIX + 'runs/' + model.digest(manifest) + '.json'
        immutable(client, bucket, key, model.encoded(manifest))
        binding = {'manifest_key': key, 'output_sha256': model.digest(output)}
        published = publish(client, bucket, {**output, 'replay': binding})
        return {'published': published, 'generated_at': stamp, 'replay': binding, 'funds': output['fund_count'],
                'retained_artifacts': count, 'signals_emitted': 0, 'paid_ai_calls': 0,
                'notifications_sent': 0, 'private_account_reads': 0, 'portfolio_writes': 0}
    except Exception as exc:
        attempt = {'contract': 'holdings-native-failed-attempt.v1', 'input': input_ref, 'compilers': compilers,
                   'generated_at': stamp, 'failure_class': type(exc).__name__, 'published': False}
        key = model.PREFIX + 'attempts/' + model.digest(attempt) + '.json'
        immutable(client, bucket, key, model.encoded(attempt))
        raise


def handle(event, client, bucket):
    headers = {'Content-Type': 'application/json', 'Cache-Control': 'no-store'}
    if event.get('action') == 'holdings_research_read':
        try:
            raw = reader(client, bucket)(model.CURRENT)
        except Exception as exc:
            if not missing(exc):
                raise
            return {'statusCode': 503, 'headers': headers, 'body': json.dumps({'status': 'not_published'})}
        return {'statusCode': 200, 'headers': headers, 'body': raw.decode('utf-8')}
    if event.get('action') != 'holdings_research_refresh' or not isinstance(event.get('probe'), dict):
        raise ValueError('Explicit retained-source research action and probe required')
    result = run(client, bucket, event['probe'])
    return {'statusCode': 200, 'headers': headers, 'body': json.dumps(result)}
