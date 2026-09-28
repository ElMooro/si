"""Portable checkpoint restart, corruption, revision and atomicity regressions."""
from copy import deepcopy
from contextlib import closing
from datetime import timedelta
import hashlib
import io
import json
from pathlib import Path
import sqlite3
import sys
import tempfile
import unittest
import zlib
import base64

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT/p) for p in ('aws/ops/checks/genealogy_native_candidate',
    'aws/ops/checks','aws/shared','tests')]
import genealogy_streamed_pipeline as pipeline
import genealogy_public_archive as archive
import genealogy_cache_checkpoint as checkpoint
import genealogy_revision_cache as revisions
from test_genealogy_public_archive import fixture, NOW
from test_genealogy_revision_cache import Client


class StorageError(Exception):
    def __init__(self, code):
        self.response = {'Error':{'Code':code}}
        super().__init__(code)


class DiskStore:
    """Isolated artifact transport for tests/runner probes; no AWS client."""
    def __init__(self, root):
        self.root = Path(root); self.root.mkdir(exist_ok=True)
        self.reads, self.writes, self.streams = [], [], []
        self.before_put = lambda kw: None

    def path(self, key):
        assert key.startswith(checkpoint.PREFIX)
        return self.root/hashlib.sha256(key.encode()).hexdigest()

    def get_object(self, **kw):
        assert kw['Bucket'] == archive.BUCKET
        path = self.path(kw['Key']); self.reads.append(kw['Key'])
        if not path.exists():
            raise StorageError('NoSuchKey')
        raw = path.read_bytes(); stream = io.BytesIO(raw); self.streams.append(stream)
        return {'Body':stream, 'ContentLength':len(raw), 'ETag':'"'+checkpoint.sha(raw)+'"'}

    def put_object(self, **kw):
        assert kw['Bucket'] == archive.BUCKET
        self.before_put(kw)
        path = self.path(kw['Key'])
        if kw.get('IfNoneMatch') == '*' and path.exists():
            raise StorageError('PreconditionFailed')
        if 'IfMatch' in kw and (not path.exists() or '"'+checkpoint.sha(path.read_bytes())+'"' != kw['IfMatch']):
            raise StorageError('PreconditionFailed')
        assert ('IfMatch' in kw) != ('IfNoneMatch' in kw)
        path.write_bytes(kw['Body']); self.writes.append(kw['Key'])
        return {}


class Checkpoints(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(); self.root = Path(self.temp.name)
        self.originals, _, _ = fixture(); self.source = Client(self.originals)
        self.inventory = self.originals.inventories()
        self.rows = revisions.revisions(self.source, self.inventory)
        self.cache = revisions.OriginalCache(self.root/'cold.sqlite')
        self.bound = self.cache.bind(self.source, self.rows)
        self.output = pipeline.collect(self.bound, self.inventory, self.root/'cold-output')
        self.store = DiskStore(self.root/'objects')
        self.cutoff = (NOW+timedelta(hours=1)).isoformat()
        self.ref = checkpoint.snapshot(self.store, archive.BUCKET, self.bound, self.cutoff)

    def tearDown(self):
        self.cache.close(); self.temp.cleanup()

    def restore(self, name='restored.sqlite', rows=None, ref=None, **kw):
        return checkpoint.restore(self.store, archive.BUCKET, self.rows if rows is None else rows,
                                  self.root/name, self.ref if ref is None else ref, **kw)

    def rewritten(self, mutate):
        doc = json.loads(self.store.path(self.ref['key']).read_bytes())
        chunks = [json.loads(self.store.path(r['artifact']['key']).read_bytes()) for r in doc['shards']]
        mutate(doc, chunks)
        for row, chunk in zip(doc['shards'], chunks):
            row['artifact'] = checkpoint.retain(self.store, archive.BUCKET, 'chunks', archive.canonical(chunk))
        return checkpoint.retain(self.store, archive.BUCKET, 'snapshots', archive.canonical(doc))

    def test_fresh_database_and_transport_restart_reproduce_every_output_byte(self):
        self.assertTrue(checkpoint.publish(self.store, archive.BUCKET, self.ref))
        store = DiskStore(self.root/'objects')  # No prior transport or SQL state.
        cache, proof = checkpoint.restore(store, archive.BUCKET, self.rows, self.root/'fresh.sqlite')
        self.source.reads.clear()
        try:
            result = pipeline.collect(cache.bind(self.source, self.rows), self.inventory, self.root/'warm-output')
            self.assertEqual(result, self.output)
            self.assertEqual(self.source.reads, [])
            self.assertEqual(proof['restored'], len(self.rows))
            self.assertTrue(all(s.closed for s in store.streams))
        finally:
            cache.close()

    def test_identical_snapshot_reuses_every_immutable_artifact(self):
        count = len(self.store.writes)
        second = checkpoint.snapshot(self.store, archive.BUCKET, self.bound, self.cutoff)
        self.assertEqual(second, self.ref)
        self.assertEqual(len(self.store.writes), count)
        self.assertFalse(any(k.endswith('current.json') for k in self.store.writes))

    def test_revision_changes_refetch_and_revalidate_original_not_cached_verdict(self):
        key = next(r['key'] for r in self.rows if '/records/' in r['key'])
        raw, at = self.originals.objects[key]
        self.originals.objects[key] = (raw.replace(b'c'*64, b'd'*64), at)
        self.source.tags[key] = '"new-same-size-time-revision"'
        rows = revisions.revisions(self.source, self.inventory)
        cache, proof = self.restore(rows=rows)
        self.source.reads.clear()
        try:
            result = archive.audit(cache.bind(self.source, rows), self.inventory)
            self.assertFalse(result['record_body_and_storage_checks_complete'])
            self.assertEqual([r['Key'] for r in self.source.reads], [key])
            self.assertEqual(proof['replaced_or_removed'], 1)
        finally:
            cache.close()

    def test_removed_original_does_not_survive_restore(self):
        key = next(r['key'] for r in self.rows if '/records/' in r['key'])
        rows = [r for r in self.rows if r['key'] != key]
        cache, proof = self.restore(rows=rows)
        try:
            self.assertEqual(proof['replaced_or_removed'], 1)
            self.assertIsNone(cache.db.execute('SELECT key FROM originals WHERE key=?', (key,)).fetchone())
        finally:
            cache.close()

    def test_one_changed_original_rewrites_only_its_shard_and_manifest(self):
        before = checkpoint.validate(self.store, archive.BUCKET, self.ref)
        key = next(r['key'] for r in self.rows if '/records/' in r['key'])
        raw, at = self.originals.objects[key]
        self.originals.objects[key] = (raw.replace(b'c'*64, b'd'*64), at)
        self.source.tags[key] = '"changed"'
        rows = revisions.revisions(self.source, self.inventory)
        bound = self.cache.bind(self.source, rows)
        bound.get_object(Bucket=archive.BUCKET, Key=key)['Body'].close()
        count = len(self.store.writes)
        ref = checkpoint.snapshot(self.store, archive.BUCKET, bound,
                                  (NOW+timedelta(hours=2)).isoformat())
        after = checkpoint.validate(self.store, archive.BUCKET, ref)
        self.assertEqual(len(self.store.writes)-count, 2)
        previous = {s['shard']:s['artifact'] for s in before['shards']}
        current = {s['shard']:s['artifact'] for s in after['shards']}
        self.assertEqual([s for s in previous if previous[s] != current[s]],
                         [checkpoint.sha(key.encode())[:2]])

    def test_restored_json_never_substitutes_for_current_semantic_validation(self):
        key = next(r['key'] for r in self.rows if '/records/' in r['key'])
        raw, at = self.originals.objects[key]
        self.originals.objects[key] = (raw.replace(b'"sizing_eligible":false', b'"sizing_eligible":true'), at)
        self.source.tags[key] = '"invalid-new-record"'
        inventory = self.originals.inventories()
        rows = revisions.revisions(self.source, inventory)
        bound = self.cache.bind(self.source, rows)
        bound.get_object(Bucket=archive.BUCKET, Key=key)['Body'].close()
        ref = checkpoint.snapshot(self.store, archive.BUCKET, bound, self.cutoff)
        cache, _ = self.restore(rows=rows, ref=ref)
        self.source.reads.clear()
        try:
            result = archive.audit(cache.bind(self.source, rows), inventory)
            self.assertFalse(result['record_body_and_storage_checks_complete'])
            self.assertEqual(self.source.reads, [])
        finally:
            cache.close()

    def test_interruption_before_manifest_cannot_advance_existing_head(self):
        checkpoint.publish(self.store, archive.BUCKET, self.ref)
        head = self.store.path(checkpoint.CURRENT).read_bytes()
        def fail(kw):
            if '/snapshots/' in kw['Key']:
                raise RuntimeError('interrupted snapshot')
        self.store.before_put = fail
        with self.assertRaisesRegex(RuntimeError, 'interrupted'):
            checkpoint.snapshot(self.store, archive.BUCKET, self.bound,
                                (NOW+timedelta(hours=2)).isoformat())
        self.assertEqual(self.store.path(checkpoint.CURRENT).read_bytes(), head)

    def test_missing_or_corrupt_final_chunk_rolls_back_all_restored_rows(self):
        doc = checkpoint.validate(self.store, archive.BUCKET, self.ref)
        path = self.store.path(doc['shards'][-1]['artifact']['key']); raw = path.read_bytes()
        for n, body in enumerate((None, raw[:-1], raw+b' ')):
            if body is None:
                path.unlink()
            else:
                path.write_bytes(body)
            dest = self.root/('failed'+str(n)+'.sqlite')
            with self.assertRaises(Exception):
                checkpoint.restore(self.store, archive.BUCKET, self.rows, dest, self.ref)
            with closing(sqlite3.connect(dest)) as db:
                self.assertEqual(db.execute('SELECT COUNT(*) FROM originals').fetchone()[0], 0)
        path.write_bytes(raw)

    def test_decompression_bomb_trailing_stream_and_wrong_body_hash_rejected(self):
        def bad_body(entry, packed):
            entry['body_zlib_base64'] = base64.b64encode(packed).decode()
        def mutate_bomb(doc, chunks):
            bad_body(chunks[0]['entries'][0], zlib.compress(b'x'*1000000))
        def mutate_trailing(doc, chunks):
            e = chunks[0]['entries'][0]
            bad_body(e, base64.b64decode(e['body_zlib_base64'])+b'trailing')
        def mutate_hash(doc, chunks):
            chunks[0]['entries'][0]['sha256'] = '0'*64
        for n, mutate in enumerate((mutate_bomb, mutate_trailing, mutate_hash)):
            ref = self.rewritten(mutate)
            with self.assertRaises(ValueError):
                self.restore('bad'+str(n)+'.sqlite', ref=ref)

    def test_duplicate_rows_shards_counts_and_revision_digest_fail_closed(self):
        mutations = [lambda d,c:d.update(entries=True),
                     lambda d,c:d.update(entries=d['entries']+1),
                     lambda d,c:d.update(revision_sha256='0'*64),
                     lambda d,c:d['revisions'].append(d['revisions'][-1]),
                     lambda d,c:c[0]['entries'].append(c[0]['entries'][0]),
                     lambda d,c:d['shards'].append(d['shards'][-1])]
        for n, mutate in enumerate(mutations):
            with self.assertRaises(ValueError):
                self.restore('invalid'+str(n)+'.sqlite', ref=self.rewritten(mutate))

    def test_unreviewed_path_boolean_length_and_weak_metadata_rejected(self):
        for n, patch in enumerate(({'key':'data/prospective-outcomes.json'}, {'bytes':True}, {'sha256':'x'*64})):
            ref = dict(self.ref, **patch)
            with self.assertRaisesRegex(ValueError, 'checkpoint_reference'):
                self.restore('reference'+str(n)+'.sqlite', ref=ref)
        bad = deepcopy(self.rows); bad[0]['etag'] = 'W/"weak"'
        with self.assertRaises(ValueError):
            self.restore(rows=bad)
        self.assertTrue(all(k.startswith(checkpoint.PREFIX) for k in self.store.reads))

    def test_existing_database_never_imported_or_executed(self):
        path = self.root/'malicious.sqlite'; path.write_bytes(b'not a trusted database')
        with self.assertRaises(FileExistsError):
            self.restore(path.name)
        self.assertEqual(path.read_bytes(), b'not a trusted database')

    def test_cache_budget_evicts_bytes_without_omitting_original_validation(self):
        cache, proof = self.restore(max_entries=1)
        self.source.reads.clear()
        try:
            result = pipeline.collect(cache.bind(self.source, self.rows), self.inventory, self.root/'limited')
            self.assertEqual(result, self.output)
            self.assertEqual(proof['cache']['entries'], 1)
            self.assertGreater(len(self.source.reads), 0)
        finally:
            cache.close()

    def test_old_and_same_clock_conflicting_snapshots_cannot_replace_head(self):
        newer = checkpoint.snapshot(self.store, archive.BUCKET, self.bound,
                                    (NOW+timedelta(hours=2)).isoformat())
        self.assertTrue(checkpoint.publish(self.store, archive.BUCKET, newer))
        self.assertFalse(checkpoint.publish(self.store, archive.BUCKET, self.ref))
        self.assertTrue(checkpoint.publish(self.store, archive.BUCKET, newer))
        self.cache.db.execute('DELETE FROM originals WHERE key=?', (self.rows[0]['key'],))
        self.cache.db.commit()
        conflicting = checkpoint.snapshot(self.store, archive.BUCKET, self.bound,
                                          (NOW+timedelta(hours=2)).isoformat())
        with self.assertRaisesRegex(ValueError, 'same_clock_conflict'):
            checkpoint.publish(self.store, archive.BUCKET, conflicting)

    def test_concurrent_newer_winner_is_preserved(self):
        checkpoint.publish(self.store, archive.BUCKET, self.ref)
        candidate = checkpoint.snapshot(self.store, archive.BUCKET, self.bound,
                                        (NOW+timedelta(hours=2)).isoformat())
        newer = checkpoint.snapshot(self.store, archive.BUCKET, self.bound,
                                    (NOW+timedelta(hours=3)).isoformat())
        def race(kw):
            if kw['Key'] == checkpoint.CURRENT:
                self.store.before_put = lambda kw: None
                checkpoint.publish(self.store, archive.BUCKET, newer)
                raise StorageError('PreconditionFailed')
        self.store.before_put = race
        self.assertFalse(checkpoint.publish(self.store, archive.BUCKET, candidate))
        current = checkpoint.head(self.store.path(checkpoint.CURRENT).read_bytes())
        self.assertEqual(current['snapshot'], newer)

    def test_corrupt_shard_cannot_be_published_or_treated_as_missing_cache(self):
        doc = checkpoint.validate(self.store, archive.BUCKET, self.ref)
        self.store.path(doc['shards'][0]['artifact']['key']).write_bytes(b'{}')
        with self.assertRaises(ValueError):
            checkpoint.publish(self.store, archive.BUCKET, self.ref)
        self.assertFalse(self.store.path(checkpoint.CURRENT).exists())

    def test_false_head_clock_and_noncanonical_manifest_cannot_restore(self):
        packet = {'contract':checkpoint.CONTRACT, 'cutoff':(NOW+timedelta(hours=2)).isoformat(),
                  'snapshot':self.ref}
        self.store.path(checkpoint.CURRENT).write_bytes(archive.canonical(packet))
        with self.assertRaisesRegex(ValueError, 'head_snapshot_clock'):
            checkpoint.restore(self.store, archive.BUCKET, self.rows, self.root/'clock.sqlite')
        raw = self.store.path(self.ref['key']).read_bytes()+b' '
        ref = checkpoint.retain(self.store, archive.BUCKET, 'snapshots', raw)
        with self.assertRaisesRegex(ValueError, 'manifest_schema'):
            self.restore('noncanonical.sqlite', ref=ref)

    def test_changed_bound_or_corrupt_local_cache_cannot_be_checkpointed(self):
        self.cache.bind(self.source, self.rows)
        with self.assertRaisesRegex(ValueError, 'obsolete_revision_binding'):
            checkpoint.snapshot(self.store, archive.BUCKET, self.bound, self.cutoff)
        bound = self.cache.bind(self.source, self.rows)
        self.cache.db.execute("UPDATE originals SET sha=? WHERE key=?", ('0'*64, self.rows[0]['key']))
        self.cache.db.commit()
        with self.assertRaisesRegex(ValueError, 'checkpoint_original_hash'):
            checkpoint.snapshot(self.store, archive.BUCKET, bound, self.cutoff)

    def test_missing_pointer_is_explicit_cold_cache(self):
        cache, proof = checkpoint.restore(self.store, archive.BUCKET, self.rows, self.root/'empty.sqlite')
        try:
            self.assertFalse(proof['checkpoint_found'])
            self.assertEqual(cache.stats()['entries'], 0)
        finally:
            cache.close()


if __name__ == '__main__':
    unittest.main()
