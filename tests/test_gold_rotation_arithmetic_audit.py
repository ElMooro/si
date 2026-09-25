from pathlib import Path
import copy,sys,unittest
ROOT=Path(__file__).resolve().parents[1];sys.path[:0]=[str(ROOT/'aws/ops/checks'),str(ROOT/'tests')]
from test_gold_rotation_model import fixture,m
from gold_rotation_arithmetic_audit import independent

class Tests(unittest.TestCase):
    def test_rational_audit_matches_original_rows_and_exact_derived_decimals(self):
        inputs,blobs,_=fixture();out=m.compile_output(inputs,blobs.__getitem__);result=independent(out,inputs,blobs.__getitem__)
        self.assertEqual(result['instruments'],8);self.assertEqual(result['original_price_rows'],6480)
        self.assertEqual(result['matched_ratio_points'],540);self.assertEqual(result['return_windows'],104)
    def test_source_ordinal_value_derived_window_and_authority_tampering_are_caught(self):
        inputs,blobs,_=fixture();out=m.compile_output(inputs,blobs.__getitem__)
        changes=[lambda p:p['instruments']['GLD']['history'][1]['source_rows']['full'].update(source_row_index=0),
            lambda p:p['instruments']['GLD']['history'][1]['source_rows']['full']['values'].update(close='500'),
            lambda p:p['ratios']['full']['returns']['20'].update(value_decimal='99'),lambda p:p.update(calls_eligible=True)]
        for change in changes:
            bad=copy.deepcopy(out);change(bad)
            with self.assertRaises(AssertionError):independent(bad,inputs,blobs.__getitem__)

if __name__=='__main__':unittest.main(verbosity=2)
