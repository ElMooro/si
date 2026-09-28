"""Synthetic sizing counterexamples; complete original retained in 6265."""
from pathlib import Path
from typing import Dict,List,Optional
import ast,hashlib,json,unittest
ROOT=Path(__file__).resolve().parents[2]
RAW=(ROOT/'tests/fixtures/pre-momentum-leaders-pump-positioning.py.txt').read_bytes();TREE=ast.parse(RAW)


def original():
    scope={'Dict':Dict,'List':List,'Optional':Optional,'print':lambda *a,**k:None}
    names={'TARGET_VOL_PER_NAME','KELLY_CAP','MAX_POSITION_PCT','MIN_POSITION_PCT','ATR_STOP_MULTIPLIER',
        'AGGRESSIVE_MAX_PER_NAME','AGGRESSIVE_MIN_PER_NAME','AGGRESSIVE_MAX_PER_SECTOR','AGGRESSIVE_N_NAMES_TARGET',
        'AGGRESSIVE_TOTAL_EXPOSURE','CATALYST_GRADE_MULTIPLIER'}
    functions={'compute_trade_framework','build_portfolio_basket','build_aggressive_basket'}
    nodes=[n for n in TREE.body if isinstance(n,ast.FunctionDef) and n.name in functions or
           isinstance(n,ast.Assign) and len(n.targets)==1 and isinstance(n.targets[0],ast.Name) and n.targets[0].id in names]
    exec(compile(ast.Module(body=nodes,type_ignores=[]),'<isolated original positioning math>','exec'),scope);return scope


def candidate(price=100,atr=1):
    ns=original();fw=ns['compute_trade_framework']('SYN_A',90,price,atr,None,None,None)
    return {'ticker':'SYN_A','pump_likelihood':90,'sizing_eligible':False,'context':{'sector':'Synthetic','macro_regime':'DEFENSIVE'},'trade_framework':fw}


class Tests(unittest.TestCase):
    def test_zero_kelly_is_forced_into_a_positive_position(self):
        p=original()['compute_trade_framework']('SYN_A',0,100,1,40,None,None)
        self.assertEqual(p['kelly_fraction'],0);self.assertEqual(p['position_size_pct'],.5)
    def test_missing_volatility_becomes_a_two_percent_sizing_assumption(self):
        p=candidate()['trade_framework'];self.assertEqual(p['vol_target_size_pct'],2);self.assertEqual(p['position_size_pct'],2)
    def test_boolean_prices_and_negative_stops_are_not_rejected(self):
        fn=original()['compute_trade_framework'];p=fn('SYN_A',90,True,True,None,None,None)
        self.assertNotIn('err',p);self.assertLess(p['stop_loss'],0)
        p=fn('SYN_A',90,1,2,None,None,None);self.assertLess(p['entry_zone']['low'],0);self.assertLess(p['stop_loss'],0)
    def test_conservative_basket_ignores_ineligible_flag_and_defensive_context(self):
        p=original()['build_portfolio_basket']([candidate()]);self.assertEqual(p['n_positions'],1);self.assertGreater(p['total_exposure'],0)
    def test_conservative_stop_risk_uses_ten_percent_even_when_declared_stop_differs(self):
        a=candidate(100,1);b=candidate(100,20);fn=original()['build_portfolio_basket']
        self.assertEqual(a['trade_framework']['stop_loss_pct'],-2);self.assertEqual(b['trade_framework']['stop_loss_pct'],-40)
        self.assertEqual(fn([a])['max_risk_at_stops_pct'],.2);self.assertEqual(fn([b])['max_risk_at_stops_pct'],.2)
    def test_missing_momentum_and_catalyst_grade_still_produce_fifteen_percent(self):
        fn=original()['build_aggressive_basket'];p=fn([candidate()],{})
        self.assertEqual(p['positions'][0]['catalyst_grade'],'B');self.assertIsNone(p['positions'][0]['momentum_score'])
        self.assertEqual(p['positions'][0]['position_pct'],15)
        repaired_null=fn([candidate()],{}, {'SYN_A':{'catalyst_grade':None,'sizing_eligible':False}})
        self.assertEqual(repaired_null['positions'][0]['position_pct'],15)
    def test_duplicate_ticker_occurrences_become_two_allocations(self):
        p=original()['build_portfolio_basket']([candidate(),candidate()])
        self.assertEqual(p['n_positions'],2);self.assertEqual([r['ticker'] for r in p['positions']],['SYN_A','SYN_A'])
    def test_past_earnings_are_treated_as_imminent(self):
        fn=original()['compute_trade_framework'];normal=fn('SYN_A',90,100,1,None,None,None);past=fn('SYN_A',90,100,1,None,None,-30)
        self.assertEqual(normal['stop_multiplier'],2);self.assertEqual(past['stop_multiplier'],1.5)
    def test_whole_original_matches_the_complete_actual_package_baseline(self):
        p=json.loads((ROOT/'docs/audit/2026-09-28/momentum-leaders-original-baseline.json').read_bytes())['source_checks']['justhodl-pump-positioning']
        self.assertEqual(len(RAW),p['bytes']);self.assertEqual(hashlib.sha256(RAW).hexdigest(),p['sha256'])


if __name__=='__main__':unittest.main(verbosity=2)
