"""Conditional directory publication; compatibility aliases are not a transaction."""
from contextlib import closing
import hashlib
import json

from directory_index import CHUNK, IndexIntegrityError, clock, manifest, validate_descriptor


class PublicationError(IndexIntegrityError):
    pass


def encoded(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False).encode('utf-8')


def code(exc):
    return str(getattr(exc, 'response', {}).get('Error', {}).get('Code', ''))


def read_current(client, bucket, prefix):
    captured = {}
    class Capture:
        def get_object(self, **kw):
            result = client.get_object(**kw)
            captured['etag'] = result.get('ETag')
            return result
    try:
        value = manifest(Capture(), bucket, prefix)
    except Exception as exc:
        if code(exc) in ('NoSuchKey', '404'):
            return None, None
        raise
    etag = captured.get('etag')
    if type(etag) is not str or not etag:
        raise PublicationError('Directory manifest ETag unavailable')
    clock(value.get('built_at'))
    if value.get('index_generation') is not None:
        validate_descriptor(value['index_generation'], prefix)
    return value, etag


def order(value):
    started = clock(value.get('build_started_at', value.get('built_at')))
    built = clock(value.get('built_at'))
    if started > built:
        raise PublicationError('Directory build starts after it finishes')
    return started


def selection(candidate, previous, now):
    if clock(candidate.get('finished_at')) > clock(now):
        raise PublicationError('Directory publication finishes in the future')
    if clock(candidate['built_at']) > clock(candidate['finished_at']):
        raise PublicationError('Directory build ends after publication finishes')
    incoming = order(candidate)
    if previous is None:
        return 'publish'
    earlier = order(previous)
    if clock(previous['built_at']) > clock(now):
        raise PublicationError('Existing directory build clock is in the future')
    if incoming < earlier or clock(candidate['built_at']) < clock(previous['built_at']):
        return 'superseded'
    if incoming == earlier:
        if encoded(candidate) == encoded(previous):
            return 'already_current'
        raise PublicationError('Conflicting directory publications have the same start clock')
    return 'publish'


def immutable(client, bucket, key, body, content_type, digest):
    try:
        client.put_object(Bucket=bucket, Key=key, Body=body,
                          ContentType=content_type, CacheControl='private, max-age=31536000, immutable',
                          Metadata={'sha256': digest}, IfNoneMatch='*')
        return
    except Exception as exc:
        if code(exc) not in ('PreconditionFailed', '412', 'ConditionalRequestConflict', '409'):
            raise
    # Existing immutable bytes are checked in full; metadata alone is not proof.
    obj = client.get_object(Bucket=bucket, Key=key)
    total = 0
    check = hashlib.sha256()
    with closing(obj['Body']) as stream:
        for chunk in iter(lambda: stream.read(CHUNK), b''):
            total += len(chunk)
            if total > len(body):
                raise PublicationError('Existing directory artifact is overlong')
            check.update(chunk)
    if total != len(body) or check.hexdigest() != digest:
        raise PublicationError('Existing directory artifact differs from its content identity')


def compatibility_alias(client, bucket, prefix, candidate, key, body, kind):
    """Best-effort monotone alias. Only the immutable manifest is atomic."""
    digest = hashlib.sha256(body).hexdigest()
    for _ in range(4):
        current, _ = read_current(client, bucket, prefix)
        if current is None or encoded(current) != encoded(candidate):
            return {'status': 'superseded', 'written': False}
        try:
            previous = client.head_object(Bucket=bucket, Key=key)
            etag = previous.get('ETag')
            if type(etag) is not str or not etag:
                raise PublicationError('Compatibility alias ETag unavailable')
            metadata = previous.get('Metadata') or {}
            old_clock = metadata.get('build-started-at')
            if old_clock is not None:
                if clock(old_clock) > order(candidate):
                    return {'status': 'superseded', 'written': False}
                if clock(old_clock) == order(candidate) and metadata.get('sha256') != digest:
                    raise PublicationError('Conflicting same-clock compatibility alias')
            condition = {'IfMatch': etag}
        except Exception as exc:
            if code(exc) not in ('NoSuchKey', '404'):
                raise
            condition = {'IfNoneMatch': '*'}
        extra = {'ContentEncoding': 'gzip'} if kind == 'application/json' else {}
        try:
            client.put_object(Bucket=bucket, Key=key, Body=body, ContentType=kind,
                              CacheControl='public, max-age=21600' if kind == 'application/json' else 'no-cache',
                              Metadata={'build-started-at': candidate['build_started_at'], 'sha256': digest},
                              **condition, **extra)
            return {'status': 'written', 'written': True}
        except Exception as exc:
            if code(exc) not in ('PreconditionFailed', '412', 'ConditionalRequestConflict', '409'):
                raise
    raise PublicationError('Compatibility alias repeatedly changed during update')


def publish(client, bucket, prefix, document, artifacts, now):
    if not callable(now):
        raise PublicationError('A current clock callback is required for publication retries')
    if prefix != 'data/symdir/' or set(artifacts) != {'docs', 'index', 'instruments'}:
        raise PublicationError('Complete native directory artifacts required')
    # Copy only the small manifest; packed populations are not copied or decoded.
    candidate = json.loads(encoded(document))
    if type(candidate.get('build_started_at')) is not str:
        raise PublicationError('Explicit build start clock required')
    pair = validate_descriptor(candidate['index_generation'], prefix)
    for name in ('docs', 'index'):
        body = artifacts[name]
        if type(body) is not bytes or len(body) != pair[name]['bytes'] or hashlib.sha256(body).hexdigest() != pair[name]['sha256']:
            raise PublicationError('Complete directory artifact differs from manifest')
    ib = artifacts['instruments']
    if type(ib) is not bytes or not ib:
        raise PublicationError('Complete instrument catalog bytes required')
    digest = hashlib.sha256(ib).hexdigest()
    instrument = {'key': prefix+'artifacts/'+digest+'/instruments.json.gz', 'bytes': len(ib), 'sha256': digest}
    candidate['instrument_generation'] = instrument
    candidate['publication_contract'] = {'version': 1, 'selection': 'conditional_manifest_by_build_start',
                                       'legacy_aliases': 'separate_compatibility_copies_not_atomic',
                                       'source_replay_verified': False, 'investment_authority': False}
    body = encoded(candidate)
    if len(body) > 1024 * 1024:
        raise PublicationError('Directory manifest exceeds existing reader bound')
    previous, etag = read_current(client, bucket, prefix)
    action = selection(candidate, previous, now())
    if action == 'superseded':
        return {**candidate, 'publication': {'status': action, 'published': False, 'aliases': {}}}
    for name, metadata, kind in [('docs', pair['docs'], 'application/octet-stream'),
                                 ('index', pair['index'], 'application/octet-stream'),
                                 ('instruments', instrument, 'application/json')]:
        immutable(client, bucket, metadata['key'], artifacts[name], kind, metadata['sha256'])
    for attempt in range(5):
        if attempt:
            previous, etag = read_current(client, bucket, prefix)
            action = selection(candidate, previous, now())
        if action in ('already_current', 'superseded'):
            break
        try:
            client.put_object(Bucket=bucket, Key=prefix+'manifest.json', Body=body,
                              ContentType='application/json', CacheControl='public, max-age=120',
                              **({'IfMatch': etag} if etag is not None else {'IfNoneMatch': '*'}))
            action = 'published'
            break
        except Exception as exc:
            if code(exc) not in ('PreconditionFailed', '412', 'ConditionalRequestConflict', '409'):
                raise
    else:
        raise PublicationError('Directory head repeatedly changed; immutable generation retained')
    aliases = {}
    if action != 'superseded':
        for name, suffix, kind in [('docs', 'docs.pkl.gz', 'application/octet-stream'),
                                   ('index', 'index.pkl.gz', 'application/octet-stream'),
                                   ('instruments', 'instruments.json.gz', 'application/json')]:
            try:
                aliases[suffix] = compatibility_alias(client, bucket, prefix, candidate, prefix+suffix, artifacts[name], kind)
            except Exception as exc:
                aliases[suffix] = {'status': 'failed', 'written': False, 'error_type': type(exc).__name__}
    failed = [key for key, value in aliases.items() if value['status'] == 'failed']
    if failed:
        # Scheduler targets discard successful return bodies. Preserve the
        # committed immutable generation, but surface copy failures through the
        # existing Lambda error/retry path instead of losing them in a response.
        raise PublicationError('Directory generation committed; compatibility alias updates failed: '+', '.join(failed))
    return {**candidate, 'publication': {'status': action, 'published': action != 'superseded',
                                        'aliases': aliases, 'aliases_complete': len(aliases) == 3 and all(r['written'] for r in aliases.values())}}
