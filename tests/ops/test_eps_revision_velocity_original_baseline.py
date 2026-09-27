from pathlib import Path
from types import SimpleNamespace
import ast,json,sys,time,unittest
ROOT=Path(__file__).resolve().parents[2];sys.path[:0]=[str(ROOT/p) for p in ('aws/ops/staged','aws/ops','aws/ops/checks')]
import ops_6249_eps_revision_velocity_original_baseline as op


def original(estimates=None,grades=None):
    raw=(ROOT/'tests/fixtures/pre-eps-revision-velocity-observations.py.txt').read_bytes();tree=ast.parse(raw)
    fake=SimpleNamespace(time=lambda:1790510400,strftime=lambda fmt:'2026',strptime=time.strptime,mktime=time.mktime)
    scope={'time':fake,'MIN_MCAP':300000000,'MIN_VELOCITY_PCT':5,
           'fetch_quote':lambda symbol:{'symbol':'OTHER','marketCap':1e9,'price':10,'name':'Fixture','currency':'JPY','timestamp':0},
           'fetch_estimates':lambda symbol:estimates if estimates is not None else [
               {'symbol':'TEST','date':'2026-12-31','epsAvg':1,'revenueAvg':100,'numAnalystsEps':5},
               {'symbol':'TEST','date':'2027-12-31','epsAvg':2,'revenueAvg':120,'numAnalystsEps':5}],
           'fetch_ratings_history':lambda symbol:grades or []}
    fns=[n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name in ('evaluate_ticker','_build_rationale')]
    exec(compile(ast.Module(body=fns,type_ignores=[]),'<isolated preserved EPS functions>','exec'),scope)
    return scope['evaluate_ticker']('TEST',1790510401)


class Tests(unittest.TestCase):
    def test_complete_original_is_pinned_without_importing_native(self):
        raw=(ROOT/'tests/fixtures/pre-eps-revision-velocity-observations.py.txt').read_bytes()
        self.assertEqual(op.source_check('justhodl-eps-revision-velocity',raw)['bytes'],16411)
        with self.assertRaises(ValueError):op.source_check('justhodl-eps-revision-velocity',raw+b'\n')
        self.assertNotIn('lambda_function',sys.modules)
    def test_different_targets_become_tiered_velocity_without_any_revision_history(self):
        out=original();self.assertEqual(out['estimates']['fy2_lift_pct'],100);self.assertEqual(out['flag'],'HIGH_VELOCITY_TIER_B')
        self.assertEqual(out['fundamentals']['market_cap'],1e9)
    def test_zero_replaced_by_alias_and_calendar_year_overwrites_target(self):
        values=[{'date':'2026-01-31','epsAvg':0,'estimatedEpsAvg':1,'revenueAvg':100},{'date':'2027-01-31','epsAvg':2,'revenueAvg':120}]
        out=original(values);self.assertEqual(out['estimates']['fy1_eps_avg'],1);self.assertEqual(out['estimates']['fy1_year'],'2026')
        values.insert(1,{'date':'2026-12-31','epsAvg':1.5,'revenueAvg':100})
        out=original(values);self.assertEqual(out['estimates']['fy1_eps_avg'],1.5)
    def test_future_rating_and_company_name_become_breadth_and_unchanged_sell_downgrade(self):
        grades=[{'date':'2027-01-01','newGrade':'Buy','previousGrade':'Hold'},
                {'date':'2026-09-26','newGrade':'Sell','previousGrade':'Sell'},
                {'date':'2026-09-26','gradingCompany':'Buy Research','previousGrade':'Hold'}]
        out=original(grades=grades)['ratings_breadth'];self.assertEqual(out['n_recent_90d'],3);self.assertEqual(out['n_upgrades'],2);self.assertEqual(out['n_downgrades'],1)


if __name__=='__main__':unittest.main(verbosity=2)
