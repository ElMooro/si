from pathlib import Path
from datetime import datetime, timezone, timedelta
import copy, sys, unittest
ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT / p) for p in ('aws/shared', 'tests')]
import sec_ftd_context as context
from test_sec_ftd_research import fixture, model


def packet():
    blobs, inputs = fixture()
    value = model.compile_output(inputs, blobs.__getitem__)['packet']
    value['generated_at'] = (datetime.now(timezone.utc) - timedelta(seconds=1)).isoformat()
    value['replay'] = {'manifest_key': model.PREFIX + 'runs/' + 'a'*64 + '.json', 'output_sha256': model.digest(value)}
    return value


class Tests(unittest.TestCase):
    def test_research_reference_survives_without_decision_authority(self):
        original = packet()
        value = context.decision_view(original)
        self.assertTrue(value['research_context']['native_reference_available'])
        self.assertEqual(value['research_context']['canonical']['replay'], original['replay'])
        self.assertEqual(value['independent_investment_votes'], 0)
        self.assertEqual(value['board'], [])
        self.assertTrue(all(value[k] is False for k in context.FLAGS))
        self.assertFalse(value['research_context']['original_provider_replay_performed_by_consumer'])
        value['research_context']['canonical']['replay']['output_sha256'] = 'changed'
        self.assertNotEqual(original['replay']['output_sha256'], 'changed')

    def test_legacy_corrupted_and_future_packets_have_no_reference_or_votes(self):
        cases = [None, {}, {'board': [{'ticker': 'ABC', 'score': 99, 'state': 'LOADED'}]}]
        for mutate in (lambda p: p.update(score=99), lambda p: p['counts'].update(original_rows=1),
                       lambda p: p['replay'].update(manifest_key='data/account.json'),
                       lambda p: p.update(generated_at='2999-01-01T00:00:00+00:00')):
            value = packet()
            mutate(value)
            cases.append(value)
        for value in cases:
            with self.subTest(value=type(value).__name__):
                guarded = context.guard(context.CURRENT, value)
                self.assertFalse(guarded['research_context']['native_reference_available'])
                self.assertEqual(guarded['board'], [])
                self.assertEqual(guarded['independent_investment_votes'], 0)
        sentinel = object()
        self.assertIs(context.guard('data/unrelated.json', sentinel), sentinel)


if __name__ == '__main__':
    unittest.main(verbosity=2)
