import ast
from copy import deepcopy
from datetime import datetime, timezone
import hashlib
import io
import json
from pathlib import Path
import unittest
from unittest.mock import patch

import holdings_native as model
import holdings_store as store


class S3Error(Exception):
    def __init__(self, code):
        self.response = {'Error': {'Code': code}}


class MemoryS3:
    def __init__(self):
        self.objects = {}; self.writes = []; self.conflict = False
    def get_object(self, Bucket, Key):
        if Key not in self.objects:
            raise S3Error('NoSuchKey')
        raw = self.objects[Key]
        return {'Body': io.BytesIO(raw), 'ETag': hashlib.sha256(raw).hexdigest()}
    def put_object(self, Bucket, Key, Body, **kwargs):
        if self.conflict:
            raise S3Error('PreconditionFailed')
        if kwargs.get('IfNoneMatch') == '*' and Key in self.objects:
            raise S3Error('PreconditionFailed')
        if 'IfMatch' in kwargs and (Key not in self.objects or hashlib.sha256(self.objects[Key]).hexdigest() != kwargs['IfMatch']):
            raise S3Error('PreconditionFailed')
        self.objects[Key] = Body; self.writes.append((Key, kwargs))


class HoldingsPublication(unittest.TestCase):
    def packet(self, stamp='2026-09-19T22:00:00Z', source='2026-09-19T21:00:00Z'):
        return {'contract': model.CONTRACT, 'generated_at': stamp, 'source_generated_at': source, **model.PERMISSION}

    def test_conditional_publication_rejects_older_source_even_with_later_build(self):
        s3 = MemoryS3(); first = self.packet()
        self.assertTrue(store.publish(s3, 'bucket', first))
        self.assertFalse(store.publish(s3, 'bucket', self.packet('2026-09-19T23:00:00Z', '2026-09-19T20:00:00Z')))
        self.assertEqual(json.loads(s3.objects[model.CURRENT]), first)
        self.assertEqual(s3.writes[0][1]['CacheControl'], 'no-store')

    def test_same_time_conflict_cannot_replace_replayed_bytes(self):
        s3 = MemoryS3(); first = self.packet(); store.publish(s3, 'bucket', first)
        with self.assertRaisesRegex(ValueError, 'Same-clock'):
            store.publish(s3, 'bucket', {**first, 'unexpected': True})
        self.assertEqual(json.loads(s3.objects[model.CURRENT]), first)

    def test_immutable_conflict_verified_not_overwritten(self):
        s3 = MemoryS3(); key = model.PREFIX + 'test.json'
        store.immutable(s3, 'bucket', key, b'original')
        store.immutable(s3, 'bucket', key, b'original')
        with self.assertRaisesRegex(ValueError, 'bytes differ'):
            store.immutable(s3, 'bucket', key, b'replacement')
        self.assertEqual(s3.objects[key], b'original')

    def test_current_read_does_not_collect_write_or_send(self):
        s3 = MemoryS3(); s3.objects[model.CURRENT] = model.encoded(self.packet())
        result = store.handle({'action': 'holdings_research_read'}, s3, 'bucket')
        self.assertEqual(result['statusCode'], 200); self.assertEqual(s3.writes, [])
        self.assertEqual(json.loads(result['body']), self.packet())

    def test_missing_research_read_returns_unavailable(self):
        s3 = MemoryS3(); result = store.handle({'action': 'holdings_research_read'}, s3, 'bucket')
        self.assertEqual(result['statusCode'], 503); self.assertEqual(s3.writes, [])

    def test_whole_legacy_snapshot_does_not_modify_source(self):
        s3 = MemoryS3(); body = b'{"legacy":{"rows":[1,2,3]},"keep":"whole"}'
        for key in store.LEGACY_KEYS:
            s3.objects[key] = body
        ref = store.preserve(s3, 'bucket'); marker = store.verified(ref, store.reader(s3, 'bucket'), 'legacy')
        self.assertEqual(len(marker['objects']), len(store.LEGACY_KEYS))
        for key in store.LEGACY_KEYS:
            self.assertEqual(s3.objects[key], body)
        self.assertTrue(all(k.startswith((store.PRIVATE, model.PREFIX)) for k, _ in s3.writes))
        self.assertEqual(s3.objects[store.PRIVATE + hashlib.sha256(body).hexdigest() + '.bin'], body)

    def test_corrupt_input_keeps_last_good_and_does_not_publish(self):
        s3 = MemoryS3(); first = self.packet(); s3.objects[model.CURRENT] = model.encoded(first)
        key = model.PREFIX + 'probes/' + 'a' * 64 + '.json'; s3.objects[key] = b'{}'
        with self.assertRaisesRegex(ValueError, 'bytes differ'):
            store.run(s3, 'bucket', {'key': key, 'sha256': 'a'*64, 'bytes': 2})
        self.assertEqual(json.loads(s3.objects[model.CURRENT]), first)
        self.assertEqual(s3.writes, [])

    def test_compile_failure_records_attempt_without_replacing_current(self):
        s3 = MemoryS3(); first = self.packet(); s3.objects[model.CURRENT] = model.encoded(first)
        raw = b'{}'; sha = hashlib.sha256(raw).hexdigest(); key = model.PREFIX + 'probes/' + sha + '.json'
        s3.objects[key] = raw
        with self.assertRaisesRegex(ValueError, 'contract differs'):
            store.run(s3, 'bucket', {'key': key, 'sha256': sha, 'bytes': 2})
        self.assertEqual(json.loads(s3.objects[model.CURRENT]), first)
        failed = [k for k in s3.objects if k.startswith(model.PREFIX + 'attempts/')]
        self.assertEqual(len(failed), 1)
        self.assertFalse(json.loads(s3.objects[failed[0]])['published'])



if __name__ == '__main__':
    unittest.main()
