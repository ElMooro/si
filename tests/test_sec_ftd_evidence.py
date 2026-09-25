from pathlib import Path
from fractions import Fraction
import sys, unittest
ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT / p) for p in ('aws/shared', 'aws/ops/checks', 'tests')]
import sec_ftd_evidence as evidence
from test_sec_ftd_research import fixture, record, model


class Tests(unittest.TestCase):
    def test_every_original_and_control_is_verified_independently(self):
        blobs, inputs = fixture()
        compiled = model.compile_output(inputs, blobs.__getitem__)
        proof = evidence.qualify(inputs, compiled, lambda ref: blobs[ref['key']])
        self.assertEqual(proof['original_rows_checked'], 36)
        self.assertEqual(proof['file_controls_checked'], 24)
        self.assertEqual(proof['independent_integer_fraction_comparisons'], 23)
        self.assertEqual(proof['missing_reported_symbols_retained'], 1)
        self.assertFalse(proof['sizing_qualified'])

    def test_rehashed_arithmetic_source_and_population_tampering_is_rejected(self):
        for kind in ('quantity', 'price', 'symbol', 'line', 'delta', 'ratio', 'prior', 'count', 'eligibility', 'controls'):
            blobs, inputs = fixture()
            compiled = model.compile_output(inputs, blobs.__getitem__)
            point = record(compiled)['observations'][1]
            positions = {'quantity': 5, 'price': 6, 'symbol': 3, 'line': 1, 'delta': 10, 'ratio': 11, 'prior': 9}
            if kind in positions:
                position = positions[kind]
                point[position] = 1 if kind == 'line' else '999'
            elif kind == 'count':
                compiled['packet']['counts']['original_rows'] -= 1
            elif kind == 'eligibility':
                compiled['packet']['calls_eligible'] = True
            else:
                compiled['packet']['sources'][0]['integrity_controls']['reported_quantity_sum'] = '0'
            # Preserve shard hash consistency to test independent meaning, not just byte checks.
            compiled['packet']['record_shards'] = {k: model.record_identity(v) for k, v in compiled['shards'].items()}
            with self.subTest(kind=kind), self.assertRaises(AssertionError):
                evidence.qualify(inputs, compiled, lambda ref: blobs[ref['key']])

    def test_integer_rounding_matches_exact_half_even_boundaries(self):
        self.assertEqual(evidence.percentage(Fraction(1, 2 * 10**12)), '0.000000000000')
        self.assertEqual(evidence.percentage(Fraction(3, 2 * 10**12)), '0.000000000002')
        self.assertEqual(evidence.percentage(Fraction(-3, 2 * 10**12)), '-0.000000000002')
        self.assertEqual(evidence.percentage(Fraction(-1, 2 * 10**12)), '0.000000000000')


if __name__ == '__main__':
    unittest.main(verbosity=2)
