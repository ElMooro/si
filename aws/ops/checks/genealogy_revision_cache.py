"""Revision-bound original-byte cache for the approved public journal only.

This caches bytes, never a validation verdict. Every consumer must run the full
current journal validators again. Fresh complete metadata is required before
reuse and must be checked again before accepting a computation. A listing is
not an atomic historical S3 snapshot. The caller owns that acceptance boundary.
"""
from datetime import datetime, timezone
import hashlib
import io
import re
import sqlite3
import threading
import zlib

from genealogy_public_archive import (
    BUCKET, PREFIX, MAX_OBJECT_BYTES, canonical, clock, require, strict_json,
    validate_inventory,
)
from prospective_journal import digest, protocol_document

CONTRACT = 'genealogy-original-revision-cache.v1'
PROTOCOL_KEY = PREFIX+'protocols/'+digest(protocol_document())+'.json'
PREFIXES = (PREFIX+'captures/', PREFIX+'records/')


def strong_etag(value):
    # Opaque entity validator, not an assumed MD5 (multipart/encrypted objects).
    require(type(value) is str and re.fullmatch(r'"[\x21\x23-\x7e]{1,512}"', value),
            'strong_object_revision_required')
    return value


def metadata(key, size, stored, etag):
    require(type(key) is str and (key == PROTOCOL_KEY or re.fullmatch(
        re.escape(PREFIX)+r'(captures|records)/[a-f0-9]{64}\.json', key)), 'unreviewed_cache_key')
    require(type(size) is int and 0 < size <= MAX_OBJECT_BYTES, 'cache_object_size')
    require(isinstance(stored, datetime) and stored.tzinfo is not None, 'cache_storage_clock')
    return {'key': key, 'bytes': size, 'last_modified': stored.astimezone(timezone.utc).isoformat(),
            'etag': strong_etag(etag)}


def revisions(client, inventories):
    """Relist all approved prefixes and reconcile every fixed-cutoff member.

    Replacements moved beyond the cutoff are missing, not silently ignored.
    Later newly added keys remain outside the explicitly fixed population.
    """
    validate_inventory(inventories)
    rows = []
    for prefix in PREFIXES:
        inventory = inventories[prefix]
        cutoff = clock(inventory['cutoff'])
        found, seen, terminal = [], set(), False
        for page in client.get_paginator('list_objects_v2').paginate(Bucket=BUCKET, Prefix=prefix):
            require(not terminal and type(page.get('IsTruncated')) is bool, 'revision_listing_incomplete')
            for obj in page.get('Contents', []):
                row = metadata(obj['Key'], obj['Size'], obj['LastModified'], obj.get('ETag'))
                require(row['key'].startswith(prefix) and row['key'] not in seen, 'revision_listing_key')
                seen.add(row['key'])
                if clock(row['last_modified']) <= cutoff:
                    found.append(row)
            terminal = page['IsTruncated'] is False
        require(terminal, 'revision_listing_incomplete')
        found.sort(key=lambda r: r['key'])
        plain = [{k: r[k] for k in ('key', 'bytes', 'last_modified')} for r in found]
        require(plain == inventory['objects'], 'original_inventory_changed')
        rows.extend(found)
    obj = client.head_object(Bucket=BUCKET, Key=PROTOCOL_KEY)
    protocol = metadata(PROTOCOL_KEY, obj['ContentLength'], obj['LastModified'], obj.get('ETag'))
    require(clock(protocol['last_modified']) <= clock(next(iter(inventories.values()))['cutoff']),
            'protocol_after_cutoff')
    rows.append(protocol)
    return sorted(rows, key=lambda r: r['key'])


def revision_digest(rows):
    return hashlib.sha256(canonical(rows)).hexdigest()


def checked_bytes(raw, row):
    require(type(raw) is bytes and len(raw) == row['bytes'], 'cached_original_length')
    document = strict_json(raw)
    require(type(document) is dict, 'cached_original_schema')
    sha = hashlib.sha256(raw).hexdigest()
    if '/records/' not in row['key']:
        require(row['key'].rsplit('/', 1)[1] == sha+'.json', 'cached_content_address')
    return sha


class OriginalCache:
    """Bounded disposable SQLite byte cache; contains no trusted verdicts.

    Reopening supports warm storage. Native cold-start persistence and stream
    compiler integration are separate work. A corrupt entry fails visibly;
    callers may explicitly discard the disposable cache and rerun originals.
    """
    def __init__(self, path, max_bytes=48*1024*1024, max_entries=10000):
        require(type(max_bytes) is int and max_bytes > 0 and type(max_entries) is int
                and max_entries > 0, 'cache_budget')
        self.lock = threading.RLock()
        self.db = sqlite3.connect(path, check_same_thread=False)
        self.db.execute('PRAGMA auto_vacuum=FULL')
        self.db.execute('PRAGMA cache_size=-4096')
        self.db.execute('PRAGMA temp_store=FILE')
        self.db.execute('CREATE TABLE IF NOT EXISTS originals (key TEXT PRIMARY KEY, revision TEXT NOT NULL, sha TEXT NOT NULL, size INTEGER NOT NULL, body BLOB NOT NULL, touched INTEGER NOT NULL)')
        self.db.commit()
        self.max_bytes, self.max_entries = max_bytes, max_entries
        self.hits = self.misses = self.evictions = self.invalidated = 0
        self.generation = 0
        self.tick = self.db.execute('SELECT COALESCE(MAX(touched),0) FROM originals').fetchone()[0]
        self.trim()

    def close(self):
        self.db.close()

    def trim(self):
        with self.lock, self.db:
            count, size = self.db.execute('SELECT COUNT(*),COALESCE(SUM(length(body)),0) FROM originals').fetchone()
            while count > self.max_entries or size > self.max_bytes:
                key, n = self.db.execute('SELECT key,length(body) FROM originals ORDER BY touched,key LIMIT 1').fetchone()
                self.db.execute('DELETE FROM originals WHERE key=?', (key,))
                count -= 1; size -= n; self.evictions += 1

    def bind(self, client, rows):
        expected, previous = {}, ''
        for row in rows:
            require(set(row) == {'key', 'bytes', 'last_modified', 'etag'}, 'revision_row_schema')
            clean = metadata(row['key'], row['bytes'], clock(row['last_modified']), row['etag'])
            require(clean == row and row['key'] > previous, 'revision_row_order_or_clock')
            expected[row['key']] = (row.copy(), revision_digest(row))
            previous = row['key']
        require(PROTOCOL_KEY in expected, 'protocol_revision_missing')
        with self.lock, self.db:
            self.generation += 1
            for key, revision in self.db.execute('SELECT key,revision FROM originals').fetchall():
                if key not in expected or expected[key][1] != revision:
                    self.db.execute('DELETE FROM originals WHERE key=?', (key,))
                    self.invalidated += 1
        return BoundClient(self, client, expected, self.generation)

    def stats(self):
        with self.lock:
            count, size = self.db.execute('SELECT COUNT(*),COALESCE(SUM(length(body)),0) FROM originals').fetchone()
            return {'hits': self.hits, 'misses': self.misses, 'invalidated': self.invalidated,
                    'evicted': self.evictions, 'entries': count, 'compressed_bytes': size,
                    'compressed_byte_budget': self.max_bytes, 'entry_budget': self.max_entries}

    def fetch(self, client, row, revision, generation):
        key = row['key']
        with self.lock, self.db:
            require(generation == self.generation, 'obsolete_revision_binding')
            self.tick += 1
            saved = self.db.execute('SELECT revision,sha,size,body FROM originals WHERE key=?', (key,)).fetchone()
            if saved is not None:
                require(saved[0] == revision and saved[2] == row['bytes'], 'cache_revision_conflict')
                decoder = zlib.decompressobj()
                raw = decoder.decompress(saved[3], row['bytes']+1)
                require(decoder.eof and not decoder.unused_data and not decoder.unconsumed_tail,
                        'corrupt_compressed_original')
                require(checked_bytes(raw, row) == saved[1], 'corrupt_original_hash')
                self.db.execute('UPDATE originals SET touched=? WHERE key=?', (self.tick, key))
                self.hits += 1
                return raw
            self.misses += 1
        obj = client.get_object(Bucket=BUCKET, Key=key, IfMatch=row['etag'])
        stream = obj['Body']
        try:
            require(metadata(key, obj.get('ContentLength'), obj.get('LastModified'), obj.get('ETag')) == row,
                    'original_changed_during_read')
            chunks, total = [], 0
            while True:
                chunk = stream.read(min(65536, row['bytes']+1-total))
                if not chunk:
                    break
                chunks.append(chunk); total += len(chunk)
                require(total <= row['bytes'], 'original_exceeds_metadata')
            raw = b''.join(chunks)
        finally:
            stream.close()
        sha = checked_bytes(raw, row)
        compressed = zlib.compress(raw, 1)
        with self.lock:
            require(generation == self.generation, 'obsolete_revision_binding')
        # Oversized objects are validated and returned, but never admitted.
        if len(compressed) <= self.max_bytes:
            with self.lock, self.db:
                require(generation == self.generation, 'obsolete_revision_binding')
                self.tick += 1
                self.db.execute('INSERT OR REPLACE INTO originals VALUES (?,?,?,?,?,?)',
                                (key, revision, sha, len(raw), compressed, self.tick))
                self.trim()
        return raw


class BoundClient:
    def __init__(self, cache, client, expected, generation):
        self.cache, self.client, self.expected = cache, client, expected
        self.generation = generation

    def get_object(self, **kwargs):
        require(set(kwargs) == {'Bucket', 'Key'} and kwargs['Bucket'] == BUCKET, 'unreviewed_cache_request')
        require(kwargs['Key'] in self.expected, 'unlisted_original_read')
        row, revision = self.expected[kwargs['Key']]
        raw = self.cache.fetch(self.client, row, revision, self.generation)
        return {'Body': io.BytesIO(raw), 'ContentLength': len(raw),
                'LastModified': clock(row['last_modified']), 'ETag': row['etag']}
