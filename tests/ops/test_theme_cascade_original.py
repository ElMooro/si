"""Isolated original arithmetic/selection counterexamples; no native or private I/O."""
from pathlib import Path
from typing import Optional
from copy import deepcopy
import ast,hashlib,json,math,unittest
ROOT=Path(__file__).resolve().parents[2]
RAW=(ROOT/'tests/fixtures/pre-momentum-leaders-theme-cascade.py.txt').read_bytes();TREE=ast.parse(RAW)


def original():
    ns={'Optional':Optional}
    names=('build_theme_heat_index','compute_flow_multiplier','compute_position_sizing','scan_laggards_in_hot_themes')
    nodes=[n for n in TREE.body if isinstance(n,ast.FunctionDef) and n.name in names]
    exec(compile(ast.Module(body=nodes,type_ignores=[]),'<isolated original cascade arithmetic>','exec'),ns)
    return ns


def earnings(items):
    handler=next(n for n in TREE.body if isinstance(n,ast.FunctionDef) and n.name=='lambda_handler')
    block=next(n for n in handler.body if isinstance(n,ast.Try) and 'earnings_set.add' in ast.unparse(n))
    ns={'earnings_set':set(),'_read_json':lambda key:{'calendar':items},'print':lambda *a:None}
    exec(compile(ast.Module(body=[block],type_ignores=[]),'<synthetic earnings window>','exec'),ns)
    return ns['earnings_set']


class Tests(unittest.TestCase):
    def test_complete_original_matches_captured_actual_package(self):
        b=json.loads((ROOT/'docs/audit/2026-09-28/momentum-leaders-original-baseline.json').read_bytes())['source_checks']['justhodl-theme-cascade']
        self.assertEqual(len(RAW),b['bytes']);self.assertEqual(hashlib.sha256(RAW).hexdigest(),b['sha256'])
    def test_missing_portfolio_and_explicit_ineligibility_still_produce_sizes(self):
        f=original()['compute_position_sizing']
        self.assertEqual(f({})['final_pct'],2)
        self.assertEqual(f({'combined_score':80,'sizing_eligible':False,'forecast_qualified':False})['final_pct'],5)
    def test_earnings_membership_increases_size_without_any_return_risk_evidence(self):
        f=original()['compute_position_sizing'];c={'ticker':'SYNA','combined_score':80}
        self.assertEqual(f(c)['final_pct'],5);self.assertEqual(f(c,{'SYNA'})['final_pct'],6.5)
    def test_same_day_earnings_disappears_while_negative_and_boolean_days_qualify(self):
        items=[{'ticker':'TODAY','days_out':0},{'ticker':'PAST','days_out':-10},{'ticker':'BOOL','days_out':True},{'ticker':'FUTURE','days_out':5}]
        self.assertEqual(earnings(items),{'PAST','BOOL'})
    def test_missing_flow_becomes_measured_zero_in_the_original_helper(self):
        p=original()['compute_flow_multiplier']('SYNA',{})
        self.assertEqual(p['aggregate_flow_5d_usd'],0);self.assertEqual(p['aggregate_flow_21d_usd'],0)
        self.assertEqual(p['n_etfs_holding'],0);self.assertEqual(p['multiplier'],1)
    def test_boolean_and_negative_ranks_receive_top_five_multiplier(self):
        f=original()['build_theme_heat_index']
        for rank in (True,-1):
            p=f({'all_themes':[{'ticker':'ETF1','rs_rank_20d':rank}]})['ETF1']
            self.assertEqual(p['multiplier'],1.35)
    def test_duplicate_etf_order_changes_the_heat_index(self):
        f=original()['build_theme_heat_index'];a={'ticker':'ETF1','momentum_score':90};b={'ticker':'ETF1','momentum_score':0}
        self.assertEqual(f({'all_themes':[a,b]})['ETF1']['multiplier'],1)
        self.assertEqual(f({'all_themes':[b,a]})['ETF1']['multiplier'],1.6)
    def test_nonfinite_input_survives_to_nonstandard_json(self):
        p=original()['build_theme_heat_index']({'all_themes':[{'ticker':'ETF1','momentum_score':float('nan')}]})
        self.assertTrue(math.isnan(p['ETF1']['momentum_score']))
        self.assertIn('NaN',json.dumps(p))
    def test_genuine_zero_legacy_score_is_replaced_by_tier_default(self):
        handler=next(n for n in TREE.body if isinstance(n,ast.FunctionDef) and n.name=='lambda_handler')
        loop=next(n for n in handler.body if isinstance(n,ast.For) and isinstance(n.iter,ast.Name) and n.iter.id=='sources')
        ns={**original(),'sources':[('FIRED_CONFIRMED',[{'ticker':'SYNA','composite_score':0,'current_score':0}],80)],
            'combined':[],'seen_tickers':set(),'ticker_to_etfs':{},'theme_heat':{},'exposure_lookup':{}}
        exec(compile(ast.Module(body=[loop],type_ignores=[]),'<synthetic legacy fusion loop>','exec'),ns)
        self.assertEqual(ns['combined'][0]['combined_score'],80)


if __name__=='__main__':unittest.main(verbosity=2)
