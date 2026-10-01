"""Verified provider-search SQLite staging with bounded disk rollback."""
from contextlib import closing
from pathlib import Path
import gzip
import hashlib
import json
import os
import re
import shutil
import sqlite3
import tempfile

from directory_index import CHUNK, RESERVE, SpaceCheckedWriter, clock, download

MAX_DATABASE_BYTES = 1500 * 1024 * 1024
MANIFEST_KEY = 'data/search/provider-shards.json'
REQUIRED_COLUMNS = {'id', 'provider', 'provider_name', 'title', 'key', 'kind', 'nbytes', 'age_h', 'hot'}


class WarehouseCacheError(ValueError):
    pass


def evidence(state, status):
    if status not in ('available', 'unavailable', 'not_requested'):
        raise ValueError('Explicit provider-search availability required')
    binding = state.get('identity')
    return {'status': status, 'integrity': state.get('integrity'),
            'artifact_identity': dict(binding) if isinstance(binding, dict) else None,
            'manifest_generated_at': state.get('generated_at'),
            'last_bytes_verified_at': state.get('verified_at'),
            'expanded_sha256': state.get('expanded_sha256'),
            'source_replay_verified': False, 'investment_authority': False}


def read_head(client, bucket):
    response = client.get_object(Bucket=bucket, Key=MANIFEST_KEY)
    with closing(response['Body']) as body:
        raw = body.read(1024 * 1024 + 1)
    if len(raw) > 1024 * 1024:
        raise WarehouseCacheError('Provider-search manifest exceeds byte bound')
    length = response.get('ContentLength')
    if length is not None and (type(length) is not int or length != len(raw)):
        raise WarehouseCacheError('Provider-search manifest transport differs')
    def pairs(items):
        result = {}
        for key, value in items:
            if key in result:
                raise WarehouseCacheError('Duplicate provider-search manifest field')
            result[key] = value
        return result
    def invalid(value):
        raise WarehouseCacheError('Nonfinite provider-search manifest number')
    value = json.loads(raw, object_pairs_hook=pairs, parse_constant=invalid)
    if not isinstance(value, dict):
        raise WarehouseCacheError('Provider-search manifest object required')
    return value


def identity(head, now):
    stamp = clock(head.get('generated_at'))
    if stamp > clock(now):
        raise WarehouseCacheError('Provider-search generation is in the future')
    meta = head.get('index')
    if not isinstance(meta, dict):
        raise WarehouseCacheError('Provider-search index metadata unavailable')
    key = meta.get('key')
    if type(key) is not str or not re.fullmatch(
            r'data/search/(?:index/)?provider-search(?:-\d{8}T\d{6}Z-[a-f0-9]{12})?\.sqlite\.gz', key):
        raise WarehouseCacheError('Unsupported provider-search object location')
    digest = meta.get('sha256')
    if type(digest) is not str or not re.fullmatch('[a-f0-9]{64}', digest):
        raise WarehouseCacheError('Provider-search compressed digest required')
    if meta.get('format') != 'sqlite-fts5+gzip':
        raise WarehouseCacheError('Unsupported provider-search format')
    for name in ('bytes', 'uncompressed_bytes'):
        if type(meta.get(name)) is not int or meta[name] <= 0:
            raise WarehouseCacheError('Positive typed provider-search sizes required')
    if meta['uncompressed_bytes'] > MAX_DATABASE_BYTES:
        raise WarehouseCacheError('Provider-search database exceeds existing 1500 MiB bound')
    # Identity uses complete artifact metadata, never the publication clock alone.
    return {name: meta[name] for name in ('key', 'sha256', 'bytes', 'uncompressed_bytes', 'format')}


def expand(packed, target, expected_bytes, expected_digest=None):
    digest = hashlib.sha256()
    length = 0
    with gzip.open(packed, 'rb') as src, open(target, 'xb') as dst:
        while True:
            chunk = src.read(min(CHUNK, expected_bytes - length + 1))
            if not chunk:
                break
            length += len(chunk)
            if length > expected_bytes:
                raise WarehouseCacheError('Provider-search expansion exceeds declared size')
            if shutil.disk_usage(target.parent).free < len(chunk) + RESERVE:
                raise WarehouseCacheError('Insufficient provider-search expansion space')
            dst.write(chunk)
            digest.update(chunk)
    actual = digest.hexdigest()
    if length != expected_bytes or (expected_digest is not None and actual != expected_digest):
        raise WarehouseCacheError('Provider-search expanded bytes differ')
    return actual


def inspect_database(path):
    # A closed producer database is opened read-only; no schema/row is modified.
    con = sqlite3.connect(path.resolve().as_uri() + '?mode=ro&immutable=1', uri=True)
    try:
        con.execute('PRAGMA trusted_schema=OFF')
        cursor = con.execute('PRAGMA quick_check')
        if cursor.fetchone() != ('ok',) or cursor.fetchone() is not None:
            raise WarehouseCacheError('Provider-search database integrity check failed')
        table = con.execute("SELECT type,sql FROM sqlite_master WHERE name='docs'").fetchone()
        if table is None or table[0] != 'table' or not re.search(r'\busing\s+fts5\s*\(', table[1] or '', re.I):
            raise WarehouseCacheError('Provider-search FTS5 table required')
        columns = {row[1] for row in con.execute('PRAGMA table_info(docs)')}
        if not REQUIRED_COLUMNS <= columns:
            raise WarehouseCacheError('Provider-search columns incomplete')
        con.execute('SELECT id,provider,provider_name,title,key,kind,nbytes,age_h,hot FROM docs LIMIT 0')
    finally:
        con.close()


def checkpoint(active, packed):
    length = 0
    digest = hashlib.sha256()
    with open(active, 'rb') as src, open(packed, 'xb') as dst:
        with gzip.GzipFile(fileobj=SpaceCheckedWriter(dst, packed), mode='wb', compresslevel=1) as archive:
            for chunk in iter(lambda: src.read(CHUNK), b''):
                length += len(chunk)
                digest.update(chunk)
                archive.write(chunk)
    verify = hashlib.sha256()
    size = 0
    with gzip.open(packed, 'rb') as archive:
        for chunk in iter(lambda: archive.read(CHUNK), b''):
            size += len(chunk)
            verify.update(chunk)
    if size != length or verify.hexdigest() != digest.hexdigest():
        raise WarehouseCacheError('Provider-search rollback verification failed')
    return length, digest.hexdigest()


def materialize(state, client, bucket, active, now, force=False):
    active = Path(active)
    head = read_head(client, bucket)
    binding = identity(head, now)
    if state.get('generated_at') and clock(head['generated_at']) < clock(state['generated_at']):
        raise WarehouseCacheError('Provider-search manifest clock regressed')
    if (not force and state.get('path') == str(active) and state.get('identity') == binding
            and active.is_file() and active.stat().st_size == binding['uncompressed_bytes']):
        state['generated_at'] = head['generated_at']
        return str(active)
    with tempfile.TemporaryDirectory(prefix='jh-provider-search-', dir=active.parent) as directory:
        folder = Path(directory)
        packed, partial, rollback = [folder / name for name in ('candidate.gz', 'candidate.sqlite', 'rollback.gz')]
        download(client, bucket, binding['key'], packed, binding)
        removed_old = False
        original = None
        if shutil.disk_usage(folder).free < binding['uncompressed_bytes'] + RESERVE:
            if not active.is_file():
                raise WarehouseCacheError('Insufficient provider-search staging space')
            original = checkpoint(active, rollback)
            if shutil.disk_usage(folder).free + original[0] < binding['uncompressed_bytes'] + RESERVE:
                raise WarehouseCacheError('Insufficient space even with verified rollback')
            active.unlink()
            removed_old = True
        try:
            expanded_digest = expand(packed, partial, binding['uncompressed_bytes'])
            inspect_database(partial)
            os.replace(partial, active)
        except Exception as exc:
            error = str(exc) if isinstance(exc, WarehouseCacheError) else type(exc).__name__
            exc.__traceback__ = None
            if removed_old:
                partial.unlink(missing_ok=True)
                packed.unlink(missing_ok=True)
                restored = folder / 'restored.sqlite'
                expand(rollback, restored, original[0], original[1])
                os.replace(restored, active)
            raise WarehouseCacheError('Provider-search refresh rejected: ' + error) from None
        state.update({'generated_at': head['generated_at'], 'path': str(active), 'identity': binding,
                      'expanded_sha256': expanded_digest, 'verified_at': now,
                      'integrity': 'hash_and_sqlite_structure_checked', 'investment_authority': False})
        return str(active)
