from pathlib import Path
from unittest.mock import patch
import sys, unittest
ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT / 'aws/shared'), str(ROOT / 'tests')]
import short_volume_research_model as model
import short_volume_research_store as store
from test_short_volume_research_model import fixture
from test_option_flow_store import S3


class Tests(unittest.TestCase):
    def candidate(self):
        inputs, objects = fixture()
        client = S3(objects)
        compiled = model.compile_output(inputs, objects.__getitem__)
        ref = store.retain(client, 'bucket', inputs, compiled)
        return client, inputs, compiled, ref

    def test_every_source_and_shard_replays_without_writes_or_provider_access(self):
        client, inputs, compiled, ref = self.candidate()
        before = len(client.writes)
        self.assertEqual(store.replay(ref, store.reader(client, 'bucket')), compiled)
        self.assertEqual(len(client.writes), before)
        self.assertNotIn(model.CURRENT, client.data)
        self.assertTrue(all(w['Key'].startswith(model.PREFIX) for w in client.writes))

    def test_identical_retention_is_idempotent_and_conflict_is_not_assumed_safe(self):
        client, inputs, compiled, ref = self.candidate()
        self.assertEqual(store.retain(client, 'bucket', inputs, compiled), ref)
        client.data[ref['manifest_key']] += b' '
        with self.assertRaisesRegex(ValueError, 'readback differs'):
            store.retain(client, 'bucket', inputs, compiled)

    def test_invalid_complete_shard_set_fails_before_first_write(self):
        inputs, objects = fixture()
        client = S3(objects)
        compiled = model.compile_output(inputs, objects.__getitem__)
        compiled['packet']['record_shards'][model.bucket('AAPL')]['bytes'] += 1
        with self.assertRaises(ValueError):
            store.retain(client, 'bucket', inputs, compiled)
        self.assertEqual(client.writes, [])

    def test_tampered_output_shard_compiler_and_original_fail(self):
        for kind in ('output', 'shard', 'compiler', 'original'):
            client, inputs, compiled, ref = self.candidate()
            run = model.strict(client.data[ref['manifest_key']])
            key = {'output': run['output']['key'],
                   'shard': compiled['packet']['record_shards'][model.bucket('AAPL')]['key'],
                   'compiler': next(iter(run['compilers'].values()))['key'],
                   'original': inputs['captures']['daily:' + model.source.day('2026-07-26')]['original']['key']}[kind]
            client.data[key] += b' '
            with self.assertRaises(ValueError):
                store.replay(ref, store.reader(client, 'bucket'))

    def test_exact_compiler_set_required_and_other_paths_are_unreadable(self):
        client, inputs, compiled, ref = self.candidate()
        with patch.object(store, 'COMPILERS', store.COMPILERS[:-1]):
            with self.assertRaisesRegex(ValueError, 'Exact compiler set'):
                store.replay(ref, store.reader(client, 'bucket'))
        for key in (model.CURRENT, 'data/trade-tickets.json', model.PRIVATE + 'requests/' + '0' * 64 + '.json'):
            with self.assertRaises(ValueError):
                store.reader(client, 'bucket')(key)
            with self.assertRaises(ValueError):
                store.put_immutable(client, 'bucket', key, b'{}')


if __name__ == '__main__':
    unittest.main(verbosity=2)
