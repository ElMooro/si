"""Preserve complete cycle projections and conditionally replace public heads.

This is derived-output retention, not original provider or point-in-time replay.
The two public projections are explicitly not an atomic transaction.
"""
from datetime import datetime, timezone
from pathlib import Path
import gzip
import hashlib
import json
import zlib

HEAD = 'data/cycle/features.json.gz'
MANIFEST = 'data/cycle/features-manifest.json'
PRIVATE = 'audit-private/20260909-originals/cycle-feature-research/'
LIMIT = 64 * 1024 * 1024
DECODED_LIMIT = 256 * 1024 * 1024
PERMISSIONS = ('calls_eligible', 'sizing_eligible', 'execution_eligible', 'forecast_qualified')


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def encode(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=False, allow_nan=False).encode('utf-8')


def strict(raw):
    def pairs(rows):
        result = {}
        for key, value in rows:
            if key in result:
                raise ValueError('Duplicate cycle JSON key')
            result[key] = value
        return result
    def invalid(_):
        raise ValueError('Nonfinite cycle JSON')
    result = json.loads(raw.decode('utf-8'), object_pairs_hook=pairs, parse_constant=invalid)
    encode(result)
    return result


def clock(value):
    value = datetime.fromisoformat(value.replace('Z', '+00:00'))
    if value.tzinfo is None:
        raise ValueError('Aware cycle publication clock required')
    return value.astimezone(timezone.utc)


def bounded(stream):
    try:
        parts, size = [], 0
        while True:
            part = stream.read(min(65536, LIMIT + 1 - size))
            if not part:
                break
            size += len(part)
            if size > LIMIT:
                raise ValueError('Whole cycle representation exceeds bound')
            parts.append(part)
        return b''.join(parts)
    finally:
        stream.close()


def decoded(raw):
    if raw[:2] != b'\x1f\x8b':
        return raw
    decoder = zlib.decompressobj(16 + zlib.MAX_WBITS)
    out = decoder.decompress(raw, DECODED_LIMIT + 1)
    if len(out) > DECODED_LIMIT or not decoder.eof or decoder.unused_data or decoder.unconsumed_tail:
        raise ValueError('Incomplete or excessive gzip cycle representation')
    return out


def read(client, bucket, key):
    if key not in (HEAD, MANIFEST):
        raise ValueError('Only public cycle predecessor keys allowed')
    try:
        obj = client.get_object(Bucket=bucket, Key=key)
    except Exception as exc:
        if str(getattr(exc, 'response', {}).get('Error', {}).get('Code')) in ('NoSuchKey', '404'):
            return None
        raise
    raw = bounded(obj['Body'])
    if not obj.get('ETag') or type(obj.get('ContentLength')) is not int or len(raw) != obj['ContentLength']:
        raise ValueError('Complete versioned predecessor required')
    value = strict(decoded(raw))
    if not isinstance(value, dict):
        raise ValueError('Complete cycle object required')
    return {'raw': raw, 'doc': value, 'etag': obj['ETag']}


def retain(client, bucket, raw):
    if not 0 < len(raw) <= LIMIT:
        raise ValueError('Whole retained representation required')
    ref = {'key': PRIVATE + sha(raw) + '.bin', 'sha256': sha(raw), 'bytes': len(raw)}
    try:
        client.put_object(Bucket=bucket, Key=ref['key'], Body=raw, IfNoneMatch='*',
                          ContentType='application/octet-stream', CacheControl='no-store')
    except Exception as exc:
        if str(getattr(exc, 'response', {}).get('Error', {}).get('Code')) not in ('409', '412', 'ConditionalRequestConflict', 'PreconditionFailed'):
            raise
    if bounded(client.get_object(Bucket=bucket, Key=ref['key'])['Body']) != raw:
        raise ValueError('Retained complete cycle bytes differ')
    return ref


def begin(client, bucket, started_at):
    at = clock(started_at)
    prior = {key: read(client, bucket, key) for key in (HEAD, MANIFEST)}
    if any(value and clock(value['doc']['generated_at']) >= at for value in prior.values()):
        raise ValueError('Cycle predecessor is newer than this run')
    refs = {key: retain(client, bucket, value['raw']) if value else None for key, value in prior.items()}
    return {'started_at': started_at, 'prior': prior, 'predecessors': refs}


def publish(client, bucket, state, doc, manifest):
    generated = clock(doc['generated_at'])
    if manifest['generated_at'] != doc['generated_at'] or not 0 <= (generated - clock(state['started_at'])).total_seconds() <= 600:
        raise ValueError('Matching native cycle publication clocks required')
    if any(packet.get(k) is not False for packet in (doc, manifest) for k in PERMISSIONS):
        raise ValueError('Unqualified cycle features cannot grant authority')
    names = set(doc['countries'])
    if set(manifest['coverage']) != names or manifest['features_key'] != HEAD:
        raise ValueError('Country coverage or output identity differs')
    root = Path(__file__).parent
    compiler = {name: sha((root / name).read_bytes()) for name in ('lambda_function.py', 'cycle_publication.py', 'cycle_sources.py')}
    context = {'contract': 'cycle-publication-context.v1', 'compiler_sha256': compiler,
               'predecessors': state['predecessors'], 'public_projections_atomic': False,
               'original_source_replay_verified': False,
               'scope': 'Complete derived output and predecessor retention; source_evidence identifies this run’s acquisitions. Independent original-source replay, definitions and model qualification remain open.'}
    output = {**doc, 'publication_context': context}
    plain = encode(output)
    if len(plain) > DECODED_LIMIT:
        raise ValueError('Complete cycle output exceeds decoded bound')
    head = gzip.compress(plain, mtime=0)
    ref = retain(client, bucket, head)
    published_manifest = {**manifest, 'publication_context': context,
                          'feature_snapshot': {**ref, 'decoded_sha256': sha(plain), 'decoded_bytes': len(plain),
                                               'hash_basis': 'complete_stored_gzip_and_complete_decoded_json'}}
    meta = encode(published_manifest)
    meta_ref = retain(client, bucket, meta)
    attempt = retain(client, bucket, encode({'contract': 'cycle-publication-attempt.v1',
                     'status': 'planned_bytes_only', 'generated_at': doc['generated_at'],
                     'public_projections_atomic': False, 'predecessors': state['predecessors'],
                     'intended': {HEAD: ref, MANIFEST: meta_ref}}))
    for key, prior in state['prior'].items():
        actual = read(client, bucket, key)
        if (actual is None) != (prior is None) or actual and (actual['etag'] != prior['etag'] or actual['raw'] != prior['raw']):
            raise ValueError('Cycle predecessor changed during compilation')
    for key, raw in ((HEAD, head), (MANIFEST, meta)):
        previous = state['prior'][key]
        client.put_object(Bucket=bucket, Key=key, Body=raw, ContentType='application/json',
                          CacheControl='public, max-age=' + ('900' if key == HEAD else '300'),
                          **({'ContentEncoding': 'gzip'} if key == HEAD else {}),
                          **({'IfMatch': previous['etag']} if previous else {'IfNoneMatch': '*'}))
    return attempt
