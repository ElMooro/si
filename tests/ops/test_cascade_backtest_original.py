"""Synthetic original calculation defects; no source, state, provider or output I/O."""
from pathlib import Path
from datetime import datetime,timezone
from typing import Optional
import ast,json,hashlib,math,time,unittest
ROOT=Path(__file__).resolve().parents[2]
RAW=(ROOT/'tests/fixtures/pre-momentum-leaders-theme-cascade-backtest.py.txt').read_bytes();TREE=ast.parse(RAW)


def analysis(leaders,themes,breadth,exposure=None):
    ns={'momentum':{'leaders':leaders},'theme_rotation':{'all_themes':themes,'breadth_details':breadth,'generated_at':'2099-01-01T00:00:00Z','forecast_qualified':False},
        'exposure_lookup':exposure or {},'Optional':Optional,'print':lambda *a:None,'time':time,'t0':time.time(),'datetime':datetime,'timezone':timezone}
    nodes=[n for n in TREE.body if isinstance(n,ast.FunctionDef) and n.name in ('get_etfs_for_ticker','analyze_ticker_themes','get_best_theme_for_ticker')]
    h=next(n for n in TREE.body if isinstance(n,ast.FunctionDef) and n.name=='lambda_handler')
    start=next(i for i,n in enumerate(h.body) if isinstance(n,ast.Assign) and any(isinstance(t,ast.Name) and t.id=='leaders' for t in n.targets))
    end=next(i for i,n in enumerate(h.body) if isinstance(n,ast.Assign) and any(isinstance(t,ast.Name) and t.id=='output' for t in n.targets))
    nodes+=h.body[start:end+1]
    exec(compile(ast.Module(body=nodes,type_ignores=[]),'<isolated backtest calculation>','exec'),ns)
    return ns['output'],ns


def fixture():
    return ([{'ticker':'UP','perf_5d_pct':10},{'ticker':'DOWN','perf_5d_pct':-1}],
            [{'ticker':'ETF1','momentum_score':80,'rs_rank_20d':1,'rs_acceleration':1}],
            {'ETF1':{'constituents_perf':[{'symbol':'UP'},{'symbol':'DOWN'}]}})


class Tests(unittest.TestCase):
    def test_whole_original_matches_captured_actual_package(self):
        b=json.loads((ROOT/'docs/audit/2026-09-28/momentum-leaders-original-baseline.json').read_bytes())['source_checks']['justhodl-theme-cascade-backtest']
        self.assertEqual(len(RAW),b['bytes']);self.assertEqual(hashlib.sha256(RAW).hexdigest(),b['sha256'])
    def test_same_snapshot_and_equal_control_still_strongly_validates_future_theme_thesis(self):
        p,_=analysis(*fixture());self.assertEqual(p['pumpers_5d_stats']['pct_in_top_10'],100)
        self.assertEqual(p['control_stats']['pct_in_top_10'],100);self.assertEqual(p['lift_metrics']['pct_in_top_10_lift_pp'],0)
        self.assertIn('STRONGLY VALIDATED',p['lift_metrics']['interpretation'])
    def test_missing_membership_is_dropped_from_absolute_percentage_denominator(self):
        a,b,c=fixture();a.append({'ticker':'UNKNOWN','perf_5d_pct':10});p,_=analysis(a,b,c)
        self.assertEqual(p['big_pumpers_5d_stats']['n'],2);self.assertEqual(p['big_pumpers_5d_stats']['n_with_theme'],1)
        self.assertEqual(p['lift_metrics']['big_pumper_pct_in_top_10'],100);self.assertIn('STRONGLY VALIDATED',p['lift_metrics']['interpretation'])
    def test_empty_context_is_written_as_zero_weakness(self):
        p,_=analysis([],[],{});self.assertEqual(p['lift_metrics']['big_pumper_pct_in_top_10'],0)
        self.assertEqual(p['lift_metrics']['interpretation'],'WEAK — only 0% of big pumpers in top-10 themes')
    def test_even_sample_median_uses_upper_middle_instead_of_midpoint(self):
        _,ns=analysis(*fixture());group=[{'hot_etf':'E','in_top_10':True,'in_top_20':True,'in_top_30':True,'theme_momentum':v,'theme_rs_rank':v} for v in (1,9)]
        result=ns['stats'](group,'synthetic');self.assertEqual(result['median_theme_momentum'],9);self.assertEqual(result['median_theme_rs_rank'],9)
    def test_zero_control_rate_is_discarded_as_missing_lift(self):
        a,b,c=fixture();b += [{'ticker':'E'+str(i),'momentum_score':30,'rs_rank_20d':i+2} for i in range(10)]
        c['ETF1']['constituents_perf']=[{'symbol':'UP'}];c['E9']={'constituents_perf':[{'symbol':'DOWN'}]}
        p,_=analysis(a,b,c);self.assertEqual(p['control_stats']['pct_in_top_10'],0);self.assertIsNone(p['lift_metrics']['pct_in_top_10_lift_pp'])
    def test_duplicate_issuer_inflates_sample_and_boolean_return_becomes_control(self):
        a,b,c=fixture();p,_=analysis([a[0],a[0],dict(a[1],perf_5d_pct=False)],b,c)
        self.assertEqual(p['big_pumpers_5d_stats']['n'],2);self.assertEqual(p['control_stats']['n'],1)
    def test_duplicate_exposure_helper_inflates_reported_memberships(self):
        a,b,c=fixture();p,_=analysis(a,b,c,{'UP':{'top_etfs':[{'etf':'ETF1'},{'etf':'ETF1'}]}})
        # Existing native flow guard excludes this input; this tests the helper only.
        row=p['big_pumpers_detail'][0];self.assertEqual(row['n_etfs_holding'],2);self.assertEqual(row['n_etfs_in_top_10'],2)
    def test_full_transparency_list_truncates_analyzed_memberships_at_eight(self):
        a,_,_=fixture();themes=[{'ticker':'E'+str(i),'momentum_score':30,'rs_rank_20d':i+1} for i in range(12)]
        breadth={r['ticker']:{'constituents_perf':[{'symbol':'UP'}]} for r in themes}
        p,_=analysis(a,themes,breadth);row=p['big_pumpers_detail'][0]
        self.assertEqual(row['n_etfs_holding'],12);self.assertEqual(len(row['all_etfs_with_ranks']),8)


if __name__=='__main__':unittest.main(verbosity=2)
