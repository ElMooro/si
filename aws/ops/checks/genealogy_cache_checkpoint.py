"""Portable original-byte cache, never serialized SQL or cached model verdicts.

All stored content is untrusted JSON. Fresh complete original metadata must be
supplied on restore and reconciled again after computation. Only a disposable
cache pointer is written here; this cannot publish the research result.
"""
import base64
import hashlib
from pathlib import Path
import re
import zlib

from genealogy_public_archive import canonical, clock, require, strict_json
from genealogy_revision_cache import (
    OriginalCache, PROTOCOL_KEY, checked_bytes, metadata, revision_digest, strong_etag,
)

PREFIX = 'data/signal-genealogy-research/cache/'
CURRENT = PREFIX+'current.json'
CONTRACT = 'genealogy-byte-checkpoint.v1'
MAX_OBJECT = 64*1024*1024
MAX_COMPRESSED = 48*1024*1024
MAX_ENTRIES = 10000


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def error(exc):
    return str(getattr(exc, 'response', {}).get('Error', {}).get('Code', ''))


def conflict(exc):
    return error(exc) in ('409', '412', 'ConditionalRequestConflict', 'PreconditionFailed')


def revision_rows(rows):
    require(type(rows) is list, 'checkpoint_revision_list')
    previous, result = '', {}
    for row in rows:
        require(type(row) is dict and set(row) == {'key','bytes','last_modified','etag'},
                'checkpoint_revision_schema')
        clean = metadata(row['key'], row['bytes'], clock(row['last_modified']), row['etag'])
        require(clean == row and row['key'] > previous, 'checkpoint_revision_order')
        result[row['key']] = row
        previous = row['key']
    require(PROTOCOL_KEY in result, 'checkpoint_protocol_required')
    return result


def read_object(client, bucket, key):
    require(type(key) is str and (key == CURRENT or re.fullmatch(
        re.escape(PREFIX)+r'(chunks|snapshots)/[a-f0-9]{64}\.json', key)),
        'unreviewed_checkpoint_path')
    obj = client.get_object(Bucket=bucket, Key=key)
    stream = obj['Body']
    try:
        require(type(obj.get('ContentLength')) is int and 0 < obj['ContentLength'] <= MAX_OBJECT,
                'checkpoint_object_length')
        pieces, total = [], 0
        while True:
            piece = stream.read(min(65536, obj['ContentLength']+1-total))
            if not piece:
                break
            total += len(piece)
            require(total <= obj['ContentLength'], 'checkpoint_object_length')
            pieces.append(piece)
        require(total == obj['ContentLength'], 'checkpoint_object_length')
        return b''.join(pieces), obj.get('ETag')
    finally:
        stream.close()


def checked(client, bucket, ref, kind):
    require(kind in ('chunks', 'snapshots') and type(ref) is dict and
            set(ref) == {'key','sha256','bytes'} and type(ref['sha256']) is str and
            re.fullmatch('[a-f0-9]{64}', ref['sha256']) and
            ref['key'] == PREFIX+kind+'/'+ref['sha256']+'.json' and
            type(ref['bytes']) is int and 0 < ref['bytes'] <= MAX_OBJECT,
            'checkpoint_reference')
    raw, _ = read_object(client, bucket, ref['key'])
    require(len(raw) == ref['bytes'] and sha(raw) == ref['sha256'], 'checkpoint_reference_bytes')
    return raw


def retain(client, bucket, kind, raw):
    require(kind in ('chunks', 'snapshots') and type(raw) is bytes and 0 < len(raw) <= MAX_OBJECT,
            'checkpoint_artifact_budget')
    ref = {'key':PREFIX+kind+'/'+sha(raw)+'.json', 'sha256':sha(raw), 'bytes':len(raw)}
    try:
        client.put_object(Bucket=bucket, Key=ref['key'], Body=raw, IfNoneMatch='*',
                          ContentType='application/json', CacheControl='no-store')
    except Exception as exc:
        if not conflict(exc):
            raise
    require(checked(client, bucket, ref, kind) == raw, 'checkpoint_immutable_readback')
    return ref


def original(entry, expected):
    require(type(entry) is dict and set(entry) == {'key','sha256','body_zlib_base64'},
            'checkpoint_entry_schema')
    require(type(entry['key']) is str and entry['key'] in expected, 'checkpoint_unlisted_original')
    require(type(entry['sha256']) is str and re.fullmatch('[a-f0-9]{64}', entry['sha256']),
            'checkpoint_original_hash')
    encoded = entry['body_zlib_base64']
    require(type(encoded) is str and len(encoded) <= 4*((MAX_COMPRESSED+2)//3),
            'checkpoint_encoded_budget')
    packed = base64.b64decode(encoded, validate=True)
    require(base64.b64encode(packed).decode('ascii') == encoded and 0 < len(packed) <= MAX_COMPRESSED,
            'checkpoint_canonical_encoding')
    row = expected[entry['key']]
    decoder = zlib.decompressobj()
    raw = decoder.decompress(packed, row['bytes']+1)
    require(decoder.eof and not decoder.unused_data and not decoder.unconsumed_tail,
            'checkpoint_compressed_original')
    require(checked_bytes(raw, row) == entry['sha256'], 'checkpoint_original_hash')
    return packed, row


def validate(client, bucket, ref, consume=None):
    """Read every complete shard; callbacks must use a rollback transaction."""
    raw = checked(client, bucket, ref, 'snapshots')
    doc = strict_json(raw)
    require(type(doc) is dict and set(doc) == {'contract','cutoff','revisions',
        'revision_sha256','entries','compressed_bytes','original_bytes','shards'} and
        doc['contract'] == CONTRACT and canonical(doc) == raw, 'checkpoint_manifest_schema')
    expected = revision_rows(doc['revisions'])
    at = clock(doc['cutoff'])
    require(all(clock(row['last_modified']) <= at for row in expected.values()), 'checkpoint_cutoff')
    require(doc['revision_sha256'] == revision_digest(doc['revisions']), 'checkpoint_revision_hash')
    for key, upper in (('entries', MAX_ENTRIES), ('compressed_bytes', MAX_COMPRESSED),
                       ('original_bytes', MAX_ENTRIES*32*1024*1024)):
        require(type(doc[key]) is int and 0 <= doc[key] <= upper, 'checkpoint_total_budget')
    require(type(doc['shards']) is list and len(doc['shards']) <= 256, 'checkpoint_shards')
    previous, entries, compressed, originals = '', 0, 0, 0
    for shard in doc['shards']:
        require(type(shard) is dict and set(shard) == {'shard','artifact'} and
                type(shard['shard']) is str and re.fullmatch('[a-f0-9]{2}', shard['shard']) and
                shard['shard'] > previous, 'checkpoint_shard_order')
        previous = shard['shard']
        body = checked(client, bucket, shard['artifact'], 'chunks')
        chunk = strict_json(body)
        require(type(chunk) is dict and set(chunk) == {'contract','shard','entries'} and
                chunk['contract'] == CONTRACT and chunk['shard'] == previous and
                type(chunk['entries']) is list and 0 < len(chunk['entries']) <= MAX_ENTRIES and
                canonical(chunk) == body, 'checkpoint_shard_schema')
        previous_key = ''
        for entry in chunk['entries']:
            packed, row = original(entry, expected)
            require(row['key'] > previous_key and sha(row['key'].encode())[:2] == previous,
                    'checkpoint_original_order_or_shard')
            previous_key = row['key']
            entries += 1; compressed += len(packed); originals += row['bytes']
            require(entries <= MAX_ENTRIES and compressed <= MAX_COMPRESSED, 'checkpoint_total_budget')
            if consume is not None:
                consume(entry, row, packed)
    require((entries, compressed, originals) ==
            (doc['entries'], doc['compressed_bytes'], doc['original_bytes']), 'checkpoint_totals_differ')
    return doc


def snapshot(client, bucket, bound, cutoff):
    """Retain deterministic shards, then the manifest. Never advances CURRENT."""
    rows = [row for row, _ in bound.expected.values()]
    expected = revision_rows(rows)
    require(all(clock(row['last_modified']) <= clock(cutoff) for row in rows), 'checkpoint_cutoff')
    cache = bound.cache
    with cache.lock:
        require(bound.generation == cache.generation, 'obsolete_revision_binding')
        # SQLite remains local scratch. Only typed original data is serialized.
        keys = [row[0] for row in cache.db.execute('SELECT key FROM originals ORDER BY key')]
        require(len(keys) <= MAX_ENTRIES, 'checkpoint_total_budget')
        groups = {}
        for key in keys:
            groups.setdefault(sha(key.encode())[:2], []).append(key)
        refs, count, packed_bytes, raw_bytes = [], 0, 0, 0
        for shard, members in sorted(groups.items()):
            entries = []
            for key in members:
                revision, digest, size, packed = cache.db.execute(
                    'SELECT revision,sha,size,body FROM originals WHERE key=?', (key,)).fetchone()
                require(key in expected and revision == revision_digest(expected[key]) and
                        size == expected[key]['bytes'], 'checkpoint_cache_revision')
                entry = {'key':key, 'sha256':digest,
                         'body_zlib_base64':base64.b64encode(packed).decode('ascii')}
                original(entry, expected)
                count += 1; packed_bytes += len(packed); raw_bytes += size
                require(packed_bytes <= MAX_COMPRESSED, 'checkpoint_total_budget')
                entries.append(entry)
            raw = canonical({'contract':CONTRACT, 'shard':shard, 'entries':entries})
            refs.append({'shard':shard, 'artifact':retain(client, bucket, 'chunks', raw)})
        document = {'contract':CONTRACT, 'cutoff':cutoff, 'revisions':rows,
                    'revision_sha256':revision_digest(rows), 'entries':count,
                    'compressed_bytes':packed_bytes, 'original_bytes':raw_bytes, 'shards':refs}
        return retain(client, bucket, 'snapshots', canonical(document))


def head(raw):
    doc = strict_json(raw)
    require(type(doc) is dict and set(doc) == {'contract','cutoff','snapshot'} and
            doc['contract'] == CONTRACT and canonical(doc) == raw, 'checkpoint_head_schema')
    clock(doc['cutoff'])
    return doc


def publish(client, bucket, ref):
    """Advance only a fully read-back cache snapshot; preserve newer CAS winners."""
    doc = validate(client, bucket, ref)
    packet = {'contract':CONTRACT, 'cutoff':doc['cutoff'], 'snapshot':ref}
    raw, at = canonical(packet), clock(doc['cutoff'])
    for _ in range(4):
        try:
            old_raw, tag = read_object(client, bucket, CURRENT)
            old = head(old_raw); old_at = clock(old['cutoff'])
            if old_at > at:
                return False
            if old_at == at:
                require(old_raw == raw, 'checkpoint_same_clock_conflict')
                return True
            condition = {'IfMatch':strong_etag(tag)}
        except Exception as exc:
            if error(exc) not in ('NoSuchKey', '404'):
                raise
            condition = {'IfNoneMatch':'*'}
        try:
            client.put_object(Bucket=bucket, Key=CURRENT, Body=raw,
                              ContentType='application/json', CacheControl='no-store', **condition)
            observed, _ = read_object(client, bucket, CURRENT)
            if observed == raw:
                return True
            require(clock(head(observed)['cutoff']) > at, 'checkpoint_head_readback')
            return False
        except Exception as exc:
            if not conflict(exc):
                raise
    raise RuntimeError('checkpoint_conflict_limit')


def restore(client, bucket, rows, path, ref=None, max_bytes=MAX_COMPRESSED, max_entries=MAX_ENTRIES):
    """Restore into a NEW known-schema database, atomically, using fresh metadata.

    Every checkpoint entry is checked, including those no longer current.
    Replaced/deleted originals are omitted from this disposable cache and will
    be fetched normally. Missing CURRENT means a cold cache, not failed data.
    """
    current = revision_rows(rows)
    pointer = None
    if ref is None:
        try:
            raw, _ = read_object(client, bucket, CURRENT)
            pointer = head(raw); ref = pointer['snapshot']
        except Exception as exc:
            if error(exc) not in ('NoSuchKey', '404'):
                raise
    path = Path(path)
    # Exclusive creation rejects preexisting SQL files and symlink targets.
    with path.open('xb'):
        pass
    cache = OriginalCache(path, max_bytes=max_bytes, max_entries=max_entries)
    accepted = skipped = 0
    try:
        def consume(entry, row, packed):
            nonlocal accepted, skipped
            if current.get(row['key']) != row:
                skipped += 1
                return
            cache.tick += 1; accepted += 1
            cache.db.execute('INSERT INTO originals VALUES (?,?,?,?,?,?)',
                (row['key'], revision_digest(row), entry['sha256'], row['bytes'], packed, cache.tick))
        if ref is not None:
            with cache.db:
                doc = validate(client, bucket, ref, consume)
                if pointer is not None:
                    require(pointer['cutoff'] == doc['cutoff'], 'checkpoint_head_snapshot_clock')
        cache.trim()
        return cache, {'restored':accepted, 'replaced_or_removed':skipped,
                       'checkpoint_found':ref is not None, 'cache':cache.stats()}
    except Exception:
        cache.close()
        raise
