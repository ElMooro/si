from pathlib import Path
from copy import deepcopy
import sys, unittest, ast
ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT / 'aws/ops/checks'), str(ROOT / 'tests')]
import short_volume_evidence as evidence
from test_short_volume_research_model import fixture, model


class Tests(unittest.TestCase):
    def test_candidate_source_identity_is_complete_and_not_a_current_key(self):
        for name in ('ops_6049_short_volume_replay_candidate.py', 'ops_6051_short_volume_stable_variance_candidate.py'):
            tree = ast.parse((ROOT / 'aws/ops/staged' / name).read_text())
            node = next(n.value for n in tree.body if isinstance(n, ast.Assign) and any(isinstance(t, ast.Name) and t.id == 'SOURCE' for t in n.targets))
            ref = eval(compile(ast.Expression(node), '<candidate-reference>', 'eval'), {'model': model, '__builtins__': {}})
            self.assertEqual(len(ref['sha256']), 64)
            self.assertEqual(ref['key'], model.PRIVATE + ref['sha256'] + '.bin')
            self.assertEqual(ref['bytes'], 80917)

    def test_independent_original_and_window_qualification(self):
        inputs, objects = fixture()
        compiled = model.compile_output(inputs, objects.__getitem__)
        out = evidence.qualify(inputs, compiled, lambda ref: objects[ref['key']])
        self.assertEqual(out['original_rows_reconciled'], 61)
        self.assertEqual(out['sampled_window_arithmetic_checks'], 3)
        self.assertFalse(out['full_population_window_arithmetic_checked'])

    def test_corrupt_row_mean_pooled_sd_coverage_or_latest_is_detected(self):
        inputs, objects = fixture()
        original = model.compile_output(inputs, objects.__getitem__)
        for mutation in ('row', 'mean', 'pooled', 'sd', 'coverage', 'latest'):
            compiled = deepcopy(original)
            record = compiled['shards'][model.bucket('AAPL')]['records']['AAPL']
            window = record['windows']['60']
            if mutation == 'row':
                record['points'][0][2] = '9'
            elif mutation == 'mean':
                window['mean_daily_short_volume_pct'] = '9'
            elif mutation == 'pooled':
                window['pooled_short_volume_pct'] = '9'
            elif mutation == 'sd':
                window['sample_sd_percentage_points'] = '9'
            elif mutation == 'coverage':
                window['missing_dates'] = ['2026-01-01']
            else:
                record['latest_short_volume_pct'] = '9'
            with self.assertRaises(AssertionError, msg=mutation):
                evidence.qualify(inputs, compiled, lambda ref: objects[ref['key']])


if __name__ == '__main__':
    unittest.main(verbosity=2)
