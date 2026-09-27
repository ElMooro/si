"""Reproduce original measurement defects using isolated pure functions only."""
from pathlib import Path
import ast,sys,unittest
ROOT=Path(__file__).resolve().parents[2];sys.path[:0]=[str(ROOT/p) for p in ('aws/ops/staged','aws/ops','aws/ops/checks')]
import ops_6251_revenue_acceleration_original_baseline as op


def rows():
    return [{'date':'2026-06-30','symbol':'OTHER','reportedCurrency':'JPY','period':'FY','revenue':v,
             'grossProfit':v/2,'operatingExpenses':40,'epsdiluted':1} for v in (200,180,160,140,100,100,100,100)]


def original(values):
    tree=ast.parse((ROOT/'tests/fixtures/pre-revenue-acceleration-observations.py.txt').read_bytes())
    scope={'fetch_quarterly_income':lambda *a,**k:values,'fetch_market_cap':lambda *a,**k:1e9}
    fn=next(n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name=='evaluate_ticker')
    exec(compile(ast.Module(body=[fn],type_ignores=[]),'<isolated original revenue arithmetic>','exec'),scope)
    return scope['evaluate_ticker']({'symbol':'TEST'})


class Tests(unittest.TestCase):
    def test_whole_source_is_pinned_without_native_import(self):
        raw=(ROOT/'tests/fixtures/pre-revenue-acceleration-observations.py.txt').read_bytes()
        self.assertEqual(op.source_check('justhodl-revenue-acceleration',raw)['bytes'],17886)
        with self.assertRaises(ValueError):op.source_check('justhodl-revenue-acceleration',raw+b'\n')
        self.assertNotIn('lambda_function',sys.modules)
    def test_duplicate_annual_dates_wrong_issuer_currency_become_tiered_quarter_growth(self):
        out=original(rows())
        self.assertEqual(out['metrics']['latest_yoy_growth_pct'],100)
        self.assertEqual(out['metrics']['latest_acceleration_pp'],20)
        self.assertEqual(out['consec_accel_quarters'],3)
        self.assertEqual(out['tier'],'TIER_A_ACCELERATING')
        self.assertEqual({r['quarter_end'] for r in out['yoy_growth_history']},{'2026-06-30'})
    def test_missing_and_boolean_revenue_are_numeric_growth(self):
        for value,growth in [(None,-100),(True,-99)]:
            values=rows();values[0]['revenue']=value
            out=original(values);self.assertEqual(out['metrics']['latest_yoy_growth_pct'],growth)
    def test_true_zero_sequential_growth_disappears_and_absent_profit_becomes_zero(self):
        values=rows();values[0]['revenue']=values[1]['revenue'];values[0]['grossProfit']=None
        out=original(values);self.assertIsNone(out['metrics']['seq_growth_pct']);self.assertEqual(out['metrics']['gm_trend_pp'],-50)
    def test_eight_rows_cannot_establish_advertised_four_acceleration_intervals(self):
        out=original(rows());self.assertEqual(len(out['yoy_growth_history']),4);self.assertEqual(len(out['acceleration_history']),3)
        self.assertLess(out['consec_accel_quarters'],4)
    def test_single_quarter_times_four_is_not_trailing_year_revenue(self):
        values=rows();out=original(values)
        self.assertEqual(out['metrics']['annualized_revenue'],800)
        self.assertNotEqual(out['metrics']['annualized_revenue'],sum(r['revenue'] for r in values[:4]))


if __name__=='__main__':unittest.main(verbosity=2)
