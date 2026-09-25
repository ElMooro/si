from pathlib import Path
import sys, unittest
ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT / 'aws/shared'), str(ROOT / 'tests')]
import short_volume_reference as reference
import test_short_volume_research_store as fixture_module
from test_short_volume_research_store import model, store


class Tests(unittest.TestCase):
    def fixture(self):
        client, inputs, compiled, ref = fixture_module.Tests().candidate()
        raw = model.encoded({**compiled['packet'], 'replay': ref})
        client.data[model.CURRENT] = raw
        client.data[reference.context.ALIAS] = b'{"generated_at":"2026-09-24T12:30:00Z","names":[{"state":"SHORTS COVERING"}]}'
        return client, raw

    def test_mirror_has_identical_clocks_and_bytes_and_adds_no_independent_vote(self):
        client, raw = self.fixture()
        result = reference.run(client, 'bucket', 'mirror', 'execution')
        count = len(client.writes)
        self.assertTrue(result['published'])
        self.assertEqual(client.data[reference.context.ALIAS], raw)
        self.assertEqual(result['independent_investment_votes'], 0)
        self.assertEqual(reference.run(client, 'bucket', 'mirror', 'retry'), result)
        self.assertEqual(len(client.writes), count)
        second = reference.run(client, 'bucket', 'same-source', 'execution2')
        self.assertEqual(second['reason'], 'already_current')

    def test_tampered_canonical_preserves_predecessor_and_blocks_retry(self):
        client, raw = self.fixture()
        old = client.data[reference.context.ALIAS]
        p = model.strict(raw)
        p['counts']['source_rows'] += 1
        client.data[model.CURRENT] = model.encoded(p)
        with self.assertRaises(ValueError):
            reference.run(client, 'bucket', 'bad', 'execution')
        self.assertEqual(client.data[reference.context.ALIAS], old)
        with self.assertRaisesRegex(ValueError, 'already attempted'):
            reference.run(client, 'bucket', 'bad', 'retry')

    def test_missing_retained_run_or_changed_compiler_cannot_be_mirrored(self):
        client, raw = self.fixture()
        ref = model.strict(raw)['replay']
        del client.data[ref['manifest_key']]
        with self.assertRaises(Exception):
            reference.run(client, 'bucket', 'missing', 'execution')
        self.assertNotEqual(client.data[reference.context.ALIAS], raw)


if __name__ == '__main__':
    unittest.main(verbosity=2)
