from copy import deepcopy
from pathlib import Path
from datetime import timedelta
import ast, sys, unittest
ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT/p) for p in ('scripts', 'aws/ops/checks', 'aws/shared', 'aws/lambdas/justhodl-crisis-composite/tests')]
import verify_liquidity_arithmetic as verifier
import liquidity_flow_candidate as candidate
from test_native_research import fixtures


class Tests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.source, cls.originals = fixtures()
        cls.output = candidate.build(cls.source, cls.originals, cls.source['generated_at'])

    def test_full_original_fixture_and_independent_import_boundary(self):
        proof = verifier.verify(self.output, self.source, self.originals)
        self.assertEqual(proof['calendar_dates_checked'], 180)
        self.assertEqual(proof['comparison_windows_checked'], 4)
        self.assertEqual(proof['original_rows_checked'], sum(len(self.originals[sid]['observations']['observations']) for sid in verifier.SPECS))
        self.assertGreater(proof['exact_rational_comparisons'], 700)
        tree = ast.parse(Path(verifier.__file__).read_text(encoding='utf-8'))
        imports = {node.module for node in ast.walk(tree) if isinstance(node, ast.ImportFrom)}
        imports |= {name.name for node in ast.walk(tree) if isinstance(node, ast.Import) for name in node.names}
        self.assertEqual(imports, {'calendar', 'datetime', 'fractions', 'math'})

    def test_wrong_sign_row_unit_or_window_is_rejected(self):
        changes = [
            lambda out: out['series']['WTREGEN'].__setitem__('to_usd_bn', '1'),
            lambda out: out['series']['WALCL']['history'][0].__setitem__('original_row', -1),
            lambda out: out['comparisons']['1m'].__setitem__('baseline_valuation_date', '2000-01-01'),
            lambda out: out['calendar_history_180d'][-1]['net'].__setitem__('exact_decimal', '123'),
            lambda out: out['comparisons']['1w']['legs']['RRPONTSYD']['signed_formula_contribution'].__setitem__('value', 100),
            lambda out: out.__setitem__('sizing_eligible', True),
        ]
        for mutate in changes:
            with self.subTest(mutate=mutate):
                bad = deepcopy(self.output); mutate(bad)
                with self.assertRaises(AssertionError): verifier.verify(bad, self.source, self.originals)

    def test_per_leg_acquisition_age_does_not_refresh_with_wrapper(self):
        source = deepcopy(self.source); originals = deepcopy(self.originals)
        # Fixture acquisitions already precede the wrapper. A new wrapper must
        # not give those original receipts another 26 hours of eligibility.
        latest = max(candidate.clock(originals[sid]['acquired_at']) for sid in verifier.SPECS)
        generated = (latest+timedelta(hours=26, seconds=1)).isoformat()
        out = candidate.build(source, originals, generated)
        self.assertIsNone(out['current'])
        verifier.verify(out, source, originals)


if __name__ == '__main__': unittest.main(verbosity=2)
