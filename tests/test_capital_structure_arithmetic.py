from pathlib import Path
from copy import deepcopy
from fractions import Fraction
import sys, unittest
ROOT=Path(__file__).resolve().parents[1]
sys.path[:0]=[str(ROOT/'aws/ops/checks'),str(ROOT/'tests')]
import capital_structure_arithmetic as independent
from test_capital_structure_research import fixture,compile_fixture


class Tests(unittest.TestCase):
    def test_every_original_coordinate_and_available_or_unavailable_value_is_checked(self):
        f=fixture();compiled=compile_fixture(f)
        proof=independent.verify(f['manifest'],f['identity'],compiled,f['files'].__getitem__)
        self.assertEqual(proof['original_rows_checked'],7)
        self.assertEqual(proof['metric_comparisons'],20)
        self.assertEqual(proof['exact_rational_values_checked'],23)
        self.assertFalse(proof['production_measurement_formulas_imported'])
        self.assertTrue(proof['all_original_rows_conserved'])

    def test_mutating_displayed_values_units_sources_or_coverage_fails_independent_check(self):
        f=fixture();original=compile_fixture(f)
        changes=(
            lambda c:c['shards']['ABC']['records'][0]['measurements']['metrics']['net_common_cash_return'].update(value='999'),
            lambda c:c['shards']['ABC']['records'][0]['measurements']['metrics']['net_common_cash_return'].update(unit='EUR'),
            lambda c:c['shards']['ABC']['records'][0]['measurements']['metrics']['net_common_cash_return']['inputs'][0].update(numeric_value='999'),
            lambda c:c['shards']['ABC']['records'][0]['source_rows'].pop(),
            lambda c:c['shards']['ABC']['snapshots'].pop(),
            lambda c:c['shards']['ABC']['records'][0].update(calls_eligible=True),
            lambda c:next(iter(c['packet']['sources'].values())).update(original_bytes=0))
        for change in changes:
            candidate=deepcopy(original);change(candidate)
            with self.assertRaises((AssertionError,KeyError)):independent.verify(f['manifest'],f['identity'],candidate,f['files'].__getitem__)

    def test_bad_issuer_duplicate_missing_zero_sign_and_empty_cases_are_recomputed(self):
        def change(spec,rows):
            if spec['endpoint']=='income-statement' and spec['period']=='annual': rows[0]['cik']='999'
            if spec['endpoint']=='cash-flow-statement' and spec['period']=='quarter': rows.append(deepcopy(rows[0]))
            if spec['endpoint']=='cash-flow-statement' and spec['period']=='annual':
                rows[0]['commonStockIssuance']=None;rows[0]['operatingCashFlow']=0;rows[0]['commonStockRepurchased']=100
            if spec['endpoint']=='splits':rows.clear()
        f=fixture(change);compiled=compile_fixture(f)
        proof=independent.verify(f['manifest'],f['identity'],compiled,f['files'].__getitem__)
        self.assertEqual(proof['invalid_or_uncorroborated_rows_checked'],1)
        self.assertGreater(proof['unavailable_metrics_checked'],10)
        self.assertEqual(proof['original_rows_checked'],7)
        self.assertEqual(independent.rounded(Fraction(-1,2*10**12)),'0.000000000000')
        self.assertEqual(independent.rounded(Fraction(-3,2*10**12)),'-0.000000000002')


if __name__=='__main__':unittest.main(verbosity=2)
