from pathlib import Path
from copy import deepcopy
import ast,json,sys,unittest
ROOT=Path(__file__).resolve().parents[2];sys.path[:0]=[str(ROOT/p) for p in ('aws/ops/staged','aws/ops','aws/ops/checks')]
import ops_6247_hiring_velocity_original_baseline as op


def original(**extra):
    raw=(ROOT/'tests/fixtures/pre-hiring-velocity-statement-observations.py.txt').read_bytes();tree=ast.parse(raw)
    ns={**extra};functions=[n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name in ('fetch_employee_history','cagr','clamp','analyze')]
    exec(compile(ast.Module(body=functions,type_ignores=[]),'<isolated preserved hiring functions>','exec'),ns);return ns


class Tests(unittest.TestCase):
    def test_complete_original_source_pinned_without_native_import(self):
        raw=(ROOT/'tests/fixtures/pre-hiring-velocity-statement-observations.py.txt').read_bytes();out=op.source_check('justhodl-hiring-velocity',raw);self.assertEqual(out['bytes'],len(raw))
        with self.assertRaises(ValueError):op.source_check('justhodl-hiring-velocity',raw+b'\n')
        self.assertNotIn('lambda_function',sys.modules)
    def test_original_three_days_get_called_annual_growth_and_multi_year_cagr(self):
        rows=[{'date':'2026-09-27','employeeCount':120},{'date':'2026-09-26','employeeCount':100},{'date':'2026-09-25','employeeCount':100}]
        n=original(fmp=lambda path,*args,**kw:rows if path=='historical-employee-count' else [])
        result=n['analyze']({'symbol':'TEST'});self.assertEqual(result['headcount_yoy_pct'],20);self.assertEqual(result['headcount_accel_pp'],20);self.assertTrue(result['inflection']);self.assertGreater(result['headcount_multiyr_cagr_pct'],0)
    def test_original_discards_zero_accepts_boolean_and_erases_period_vs_release_clock(self):
        rows=[{'employeeCount':0,'periodOfReport':'2026-09-27'},{'employeeCount':True,'filingDate':'2026-09-26'},{'employeeCount':3,'periodOfReport':'2025-12-31','filingDate':'2026-09-25'}]
        n=original(fmp=lambda *a,**k:rows);out=n['fetch_employee_history']('TEST')
        self.assertEqual(len(out),2);self.assertEqual(out[0],{'date':'2026-09-26','count':1});self.assertNotIn('filingDate',out[1])
    def test_original_unaligned_income_date_currency_and_issuer_still_become_productivity(self):
        employees=[{'employeeCount':100,'date':'2026-12-31'},{'employeeCount':100,'date':'2025-12-31'},{'employeeCount':100,'date':'2024-12-31'}]
        incomes=[{'revenue':10000,'date':'2020-03-31','symbol':'OTHER','reportedCurrency':'JPY'},{'revenue':5000,'date':'2019-06-30','symbol':'OTHER','reportedCurrency':'USD'}]
        n=original(fmp=lambda path,*a,**kw:employees if path=='historical-employee-count' else incomes);out=n['analyze']({'symbol':'TEST'})
        self.assertEqual(out['revenue_per_employee'],100);self.assertEqual(out['revenue_per_employee_trend_pct'],100)


if __name__=='__main__':unittest.main(verbosity=2)
