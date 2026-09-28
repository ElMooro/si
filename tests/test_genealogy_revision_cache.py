"""Original-byte cache failures and complete synthetic journal replay."""
from copy import deepcopy
from datetime import timedelta
import hashlib
import io
from pathlib import Path
import sys
import tempfile
import unittest
import zlib

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT/'aws/shared'), str(ROOT/'aws/ops/checks')]
import genealogy_public_archive as archive
import genealogy_revision_cache as cachemod
from test_genealogy_public_archive import fixture, NOW


class Client:
    def __init__(self, store):
        self.store = store
        self.tags = {k: '"opaque-'+hashlib.sha256(v[0]).hexdigest()+'-3"' for k, v in store.objects.items()}
        self.reads, self.heads, self.streams = [], [], []
        self.mutate_pages = lambda pages: pages
        self.mutate_response = lambda obj: obj

    def head_object(self, **kw):
        assert kw['Bucket'] == archive.BUCKET
        key = kw['Key']; self.heads.append(key)
        raw, stamp = self.store.objects[key]
        return {'ContentLength': len(raw), 'LastModified': stamp, 'ETag': self.tags[key]}

    def get_object(self, **kw):
        assert kw['Bucket'] == archive.BUCKET
        key = kw['Key']; self.reads.append(kw)
        if kw['IfMatch'] != self.tags[key]:
            raise ValueError('PreconditionFailed')
        raw, stamp = self.store.objects[key]
        body = io.BytesIO(raw); self.streams.append(body)
        return self.mutate_response({'Body': body, 'ContentLength': len(raw), 'LastModified': stamp, 'ETag': self.tags[key]})

    def get_paginator(self, name):
        assert name == 'list_objects_v2'
        return self

    def paginate(self, **kw):
        assert kw['Bucket'] == archive.BUCKET and kw['Prefix'] in cachemod.PREFIXES
        rows = [{'Key': k, 'Size': len(v[0]), 'LastModified': v[1], 'ETag': self.tags[k]}
                for k, v in sorted(self.store.objects.items()) if k.startswith(kw['Prefix'])]
        # Exercise real multiple-page termination, even for two records.
        pages = [{'Contents': rows[i:i+1], 'IsTruncated': i+1 < len(rows)} for i in range(len(rows))]
        return iter(self.mutate_pages(pages or [{'Contents': [], 'IsTruncated': False}]))


class Cache(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = Path(self.tmp.name)/'originals.sqlite'
        self.store, _, _ = fixture()
        self.client = Client(self.store)
        self.inventory = self.store.inventories()
        self.cache = cachemod.OriginalCache(self.path)

    def tearDown(self):
        self.cache.close()
        self.tmp.cleanup()

    def bind(self):
        return self.cache.bind(self.client, cachemod.revisions(self.client, self.inventory))

    def test_all_originals_replay_after_reopen_without_body_reads(self):
        expected = archive.audit(self.store, self.inventory)
        first = archive.audit(self.bind(), self.inventory)
        self.assertEqual(first, expected)
        self.assertTrue(first['record_body_and_storage_checks_complete'])
        self.assertEqual(len(self.client.reads), 4)
        self.assertTrue(all(s.closed for s in self.client.streams))
        self.cache.close(); self.cache = cachemod.OriginalCache(self.path)
        self.client.reads.clear()
        second = archive.audit(self.bind(), self.inventory)
        self.assertEqual(second, expected)
        self.assertEqual(self.client.reads, [])
        self.assertEqual(self.cache.stats()['hits'], 4)

    def test_same_size_same_time_revision_change_refetches_only_that_original(self):
        archive.audit(self.bind(), self.inventory)
        key = next(k for k in self.store.objects if '/records/' in k)
        raw, stamp = self.store.objects[key]
        changed = raw.replace(b'c'*64, b'd'*64)
        self.assertNotEqual(changed, raw)
        self.assertEqual(len(changed), len(raw))
        self.store.objects[key] = (changed, stamp)
        self.client.tags[key] = '"replacement-opaque-revision"'
        self.client.reads.clear()
        result = archive.audit(self.bind(), self.inventory)
        self.assertFalse(result['record_body_and_storage_checks_complete'])
        self.assertTrue(result['reference_errors'])
        self.assertEqual([r['Key'] for r in self.client.reads], [key])
        self.assertEqual(self.cache.stats()['invalidated'], 1)

    def test_deletion_or_backdated_addition_cannot_hide_in_cached_population(self):
        archive.audit(self.bind(), self.inventory)
        key = next(k for k in self.store.objects if '/records/' in k)
        original = self.store.objects.pop(key)
        with self.assertRaisesRegex(ValueError, 'original_inventory_changed'):
            self.bind()
        self.store.objects[key] = original
        new = archive.PREFIX+'records/'+'f'*64+'.json'
        self.store.objects[new] = original; self.client.tags[new] = '"new"'
        with self.assertRaisesRegex(ValueError, 'original_inventory_changed'):
            self.bind()
        # A genuinely later addition is excluded only by the declared cutoff.
        self.store.objects[new] = (original[0], NOW+timedelta(days=1))
        self.bind()

    def test_truncated_duplicate_and_nonterminal_listings_fail(self):
        cases = [lambda p: p[:-1], lambda p: [p[0], *p],
                 lambda p: [dict(p[0], IsTruncated=True)],
                 lambda p: [dict(p[0], IsTruncated=None)]]
        for mutate in cases:
            self.client.mutate_pages = mutate
            with self.assertRaises(ValueError):
                self.bind()

    def test_weak_missing_and_unquoted_tags_are_never_reused(self):
        key = next(k for k in self.store.objects if '/captures/' in k)
        for tag in (None, '', 'W/"weak"', 'unquoted', '"bad\nheader"'):
            self.client.tags[key] = tag
            with self.assertRaisesRegex(ValueError, 'strong_object_revision_required'):
                self.bind()

    def test_changed_object_between_inventory_and_get_fails_condition(self):
        bound = self.bind()
        key = next(k for k in self.store.objects if '/records/' in k)
        self.client.tags[key] = '"changed"'
        with self.assertRaisesRegex(ValueError, 'PreconditionFailed'):
            bound.get_object(Bucket=archive.BUCKET, Key=key)
        self.assertEqual(self.cache.stats()['entries'], 0)

    def test_returned_revision_clock_length_and_truncated_body_checked_and_closed(self):
        key = next(k for k in self.store.objects if '/records/' in k)
        mutations = [lambda o: dict(o, ETag='"different"'),
                     lambda o: dict(o, LastModified=o['LastModified']+timedelta(seconds=1)),
                     lambda o: dict(o, ContentLength=o['ContentLength']+1)]
        for mutate in mutations:
            self.client.mutate_response = mutate
            with self.assertRaisesRegex(ValueError, 'original_changed_during_read'):
                self.bind().get_object(Bucket=archive.BUCKET, Key=key)
            self.assertTrue(self.client.streams[-1].closed)
        self.client.mutate_response = lambda o: o
        bound = self.bind()
        raw, stamp = self.store.objects[key]
        self.store.objects[key] = (raw[:-1], stamp)
        with self.assertRaisesRegex(ValueError, 'original_changed_during_read'):
            bound.get_object(Bucket=archive.BUCKET, Key=key)
        self.assertTrue(self.client.streams[-1].closed)

    def test_corrupt_compressed_bytes_hash_and_expansion_fail_without_network_fallback(self):
        key = next(k for k in self.store.objects if '/records/' in k)
        archive.audit(self.bind(), self.inventory)
        saved = self.cache.db.execute('SELECT body,sha FROM originals WHERE key=?', (key,)).fetchone()
        for body, sha in ((saved[0][:-1], saved[1]), (saved[0]+b'trailing', saved[1]),
                          (zlib.compress(b'x'*1000000), saved[1]), (saved[0], '0'*64)):
            self.cache.db.execute('UPDATE originals SET body=?,sha=? WHERE key=?', (body, sha, key))
            self.cache.db.commit(); self.client.reads.clear()
            with self.assertRaises(ValueError):
                self.bind().get_object(Bucket=archive.BUCKET, Key=key)
            self.assertEqual(self.client.reads, [])

    def test_cache_never_bypasses_current_semantic_validation(self):
        key = next(k for k in self.store.objects if '/records/' in k)
        raw, stamp = self.store.objects[key]
        self.store.objects[key] = (raw.replace(b'"sizing_eligible":false', b'"sizing_eligible":true'), stamp)
        self.inventory = self.store.inventories()
        first = archive.audit(self.bind(), self.inventory)
        self.assertFalse(first['record_body_and_storage_checks_complete'])
        self.client.reads.clear()
        self.assertEqual(archive.audit(self.bind(), self.inventory), first)
        self.assertEqual(self.client.reads, [])

    def test_eviction_budget_oversized_bypass_and_removed_members(self):
        self.cache.close(); self.cache = cachemod.OriginalCache(self.path, max_entries=2)
        archive.audit(self.bind(), self.inventory)
        self.assertEqual(self.cache.stats()['entries'], 2)
        self.assertGreater(self.cache.stats()['evicted'], 0)
        self.cache.close(); self.cache = cachemod.OriginalCache(self.path, max_bytes=1)
        self.assertEqual(self.cache.stats()['entries'], 0)
        self.assertTrue(archive.audit(self.bind(), self.inventory)['record_body_and_storage_checks_complete'])
        self.assertEqual(self.cache.stats()['entries'], 0)
        self.cache.close(); self.cache = cachemod.OriginalCache(self.path)
        archive.audit(self.bind(), self.inventory)
        key = next(k for k in self.store.objects if '/records/' in k)
        del self.store.objects[key]
        self.inventory = self.store.inventories()
        self.bind()
        self.assertIsNone(self.cache.db.execute('SELECT key FROM originals WHERE key=?', (key,)).fetchone())

    def test_obsolete_binding_and_unreviewed_reads_rejected(self):
        old = self.bind(); self.bind()
        with self.assertRaisesRegex(ValueError, 'obsolete_revision_binding'):
            old.get_object(Bucket=archive.BUCKET, Key=cachemod.PROTOCOL_KEY)
        for key in ('data/prospective-outcomes.json', 'data/private.json', archive.PREFIX+'records/'+'f'*64+'.json'):
            with self.assertRaisesRegex(ValueError, 'unlisted_original_read'):
                self.bind().get_object(Bucket=archive.BUCKET, Key=key)
        with self.assertRaisesRegex(ValueError, 'unreviewed_cache_request'):
            self.bind().get_object(Bucket='other', Key=cachemod.PROTOCOL_KEY)


if __name__ == '__main__':
    unittest.main()
