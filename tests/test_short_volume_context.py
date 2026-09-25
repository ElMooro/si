from pathlib import Path
from copy import deepcopy
import sys, unittest
ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT / 'aws/shared'), str(ROOT / 'tests')]
import short_volume_context as context
from test_short_volume_research_model import fixture, model


def packet():
    inputs, objects = fixture()
    value = model.compile_output(inputs, objects.__getitem__)['packet']
    return {**value, 'replay': {'manifest_key': model.PREFIX + 'runs/' + 'a' * 64 + '.json', 'output_sha256': model.digest(value)}}


class Tests(unittest.TestCase):
    def test_native_exact_digest_is_context_but_never_an_independent_vote(self):
        value = context.context(packet())
        self.assertTrue(value['native_reference_available'])
        self.assertFalse(value['current_freshness_verified_by_consumer'])
        self.assertFalse(value['original_provider_replay_performed_by_consumer'])
        self.assertEqual(value['independent_investment_votes'], 0)
        self.assertTrue(all(value[k] is False for k in context.FLAGS))

    def test_legacy_and_tampered_native_do_not_restore_scores_or_covering(self):
        values = [None, {'names': [{'ticker': 'AAPL', 'state': 'SHORTS COVERING', 'z_score': -4}],
                         'tickers': {'AAPL': {'days_to_cover': 20}}, 'squeeze_candidates': [{'symbol': 'AAPL', 'squeeze_score': 99}]}]
        wrong = packet()
        wrong['counts']['source_rows'] += 1
        values.append(wrong)
        for value in values:
            for key in (context.CURRENT, context.ALIAS):
                result = context.guard(key, value)
                self.assertFalse(result['research_context']['native_reference_available'])
                self.assertEqual(result['names'], [])
                self.assertEqual(result['tickers'], {})
                self.assertEqual(result['squeeze_candidates'], [])
                self.assertIsNone(result['days_to_cover'])

    def test_unrelated_actual_short_interest_is_preserved(self):
        original = {'ticker': 'AAPL', 'short_interest_shares': 100}
        self.assertIs(context.guard('data/short-interest.json', original), original)

    def test_future_clock_and_granted_authority_fail_even_with_matching_digest(self):
        for key, value in (('generated_at', '2099-01-01T00:00:00Z'), ('calls_eligible', True), ('days_to_cover', 8)):
            current = packet()
            current[key] = value
            current['replay']['output_sha256'] = model.digest({k: v for k, v in current.items() if k != 'replay'})
            self.assertFalse(context.context(current)['native_reference_available'])


if __name__ == '__main__':
    unittest.main(verbosity=2)
