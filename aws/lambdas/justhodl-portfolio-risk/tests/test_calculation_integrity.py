"""Whole invented malformed books and independent scenario-total oracles."""
from copy import deepcopy
from decimal import Decimal
import hashlib,json
from pathlib import Path
import sys,unittest

ROOT=Path(__file__).resolve().parents[4]
sys.path[:0]=[str(ROOT/'tests'),str(Path(__file__).parent)]
from test_risk_model import fixture,evaluate,NOW,model
from private_portfolio_test_support import Store,load

SCENARIO={'shock':{'name':'Invented shock','duration_days':1,'spy_return':-.1,'sector_returns':{'Technology':-.265}}}
PRIOR=ROOT/'tests/fixtures/pre-risk-calculation/complete-synthetic.json'


class CalculationIntegrity(unittest.TestCase):
    def test_complete_reproductions_are_retained_and_repaired(self):
        raw=PRIOR.read_bytes()
        evidence=json.loads((ROOT/'docs/audit/2026-09-30/risk-calculation-source-gap.json').read_bytes())
        self.assertEqual(hashlib.sha256(raw).hexdigest(),evidence['complete_synthetic']['sha256'])
        cases=json.loads(raw)
        for label,case in cases.items():
            original=deepcopy(case['inputs']);out=model.build(**original)
            self.assertEqual(original,case['inputs']);model.canonical(out)
            self.assertFalse(out['permissions']['sizing_eligible'])
            self.assertFalse(out['permissions']['may_recommend_trades'])
            if label in ('boolean_multiplier','null_position','string_positions'):
                self.assertEqual(out['status'],'INCOMPLETE')
                self.assertIsNone(out['holdings_risk']['var_1d_99_dollars'])
            if label=='rounded_scenario_lots':
                self.assertEqual(case['output']['historical_scenarios']['sector_shock']['projected_pnl_dollars'],-27)
                self.assertEqual(out['historical_scenarios']['sector_shock']['projected_pnl_dollars'],-26.5)

    def test_boolean_text_and_missing_multiplier_values_cannot_mean_one(self):
        for value in (True,False,'1',None,0,2,{},[]):
            s,p=fixture();s['positions'][0]['multiplier']=value;out=evaluate(s,p,SCENARIO)
            self.assertIn('UNSUPPORTED_OR_AMBIGUOUS_INSTRUMENT',out['quality']['reason_codes'])
            self.assertIsNone(out['historical_scenarios']['shock']['projected_pnl_dollars'])
        for value in (1,1.0):
            s,p=fixture();s['positions'][0]['multiplier']=value
            self.assertEqual(evaluate(s,p)['status'],'AVAILABLE_HOLDINGS_MODEL')

    def test_instrument_identifiers_are_text_not_coerced_python_values(self):
        for symbol in (True,False,None,1,[],{}):
            s,p=fixture();s['positions'][0]['symbol']=symbol
            self.assertIsNone(model.cash_equity_identity(s['positions'][0]))
            self.assertEqual(evaluate(s,p,SCENARIO)['status'],'INCOMPLETE')
        self.assertEqual(model.cash_equity_identity({'symbol':'TRUE'})['symbol'],'TRUE')
        self.assertIsNone(model.cash_equity_identity({'symbol':'AAA','asset_class':False}))

    def test_only_explicit_empty_list_means_no_positions(self):
        for positions in (None,False,True,0,'AAA',{},['AAA'],[None]):
            s,p=fixture();s['positions']=positions;out=evaluate(s,p,SCENARIO)
            self.assertEqual(out['status'],'INCOMPLETE')
            self.assertEqual(out['n_positions'],len(positions) if type(positions) is list else None)
            self.assertIsNone(out['historical_scenarios']['shock']['projected_pnl_dollars'])
        s,p=fixture();s['positions']=[]
        self.assertEqual(evaluate(s,p)['status'],'no_positions')
        del s['positions'];self.assertEqual(evaluate(s,p)['status'],'INCOMPLETE')

    def test_invalid_rows_keep_their_original_position_and_do_not_become_zero(self):
        s,p=fixture();s['positions'].insert(0,None);s['positions'].append('unknown')
        before=deepcopy(s);bundle,out=model.freeze(s,p,NOW.isoformat(),SCENARIO)
        parts=out['historical_scenarios']['shock']['per_position']
        self.assertEqual(len(parts),3)
        self.assertEqual([v.get('input_index') for v in parts],[0,None,2])
        self.assertTrue(all(v['scenario_pnl'] is None for v in parts))
        self.assertEqual(s,before);self.assertEqual(bundle['inputs']['snapshot'],before)
        self.assertEqual(model.replay(bundle),out)

    def test_nontext_sector_cannot_crash_a_dictionary_lookup(self):
        for value in ([],{},True,12):
            s,p=fixture();s['positions'][0]['sector']=value;out=evaluate(s,p,SCENARIO)
            self.assertIn('INVALID_SECTOR_CLASSIFICATION',out['quality']['reason_codes'])
            self.assertIsNone(out['historical_scenarios']['shock']['projected_pnl_dollars'])

    def test_malformed_capital_diagnostics_withhold_nav_without_erasing_holdings(self):
        for value in (None,True,0,'reason',{},[False],[{}]):
            s,p=fixture();s['capital_book']={'reason_codes':value};out=evaluate(s,p,SCENARIO)
            self.assertEqual(out['status'],'AVAILABLE_HOLDINGS_MODEL')
            self.assertEqual(out['capital_basis']['errors'],['INVALID_CAPITAL_BOOK_REASON_CODES'])
            self.assertIsNone(out['capital_basis']['nav']);self.assertIsNone(out['var_1d_99_pct'])
            self.assertGreater(out['holdings_risk']['var_1d_99_dollars'],0)

    def test_decimal_oracle_and_lot_partition_invariance(self):
        expected=float(Decimal('100')*Decimal('-0.265'))
        for count in (1,2,4,10,100):
            s,p=fixture();mv=100/count
            row={**s['positions'][0],'qty':mv/100,'market_value':mv}
            s['positions']=[deepcopy(row) for _ in range(count)]
            scenario=evaluate(s,p,SCENARIO)['historical_scenarios']['shock']
            self.assertEqual(scenario['projected_pnl_dollars'],expected)
            self.assertEqual(scenario['pct_of_gross_exposure'],-26.5)
            self.assertEqual(len(scenario['per_position']),count)
            self.assertIn('unrounded',scenario['rounding'])

    def test_long_short_and_zero_shocks_keep_signed_arithmetic(self):
        for sign in (-1,1):
            s,p=fixture();row={**s['positions'][0],'qty':sign*.01,'market_value':sign}
            s['positions']=[deepcopy(row) for _ in range(100)]
            self.assertEqual(evaluate(s,p,SCENARIO)['historical_scenarios']['shock']['projected_pnl_dollars'],-26.5*sign)
        zero=deepcopy(SCENARIO);zero['shock']['sector_returns']['Technology']=0
        s,p=fixture();out=evaluate(s,p,zero)['historical_scenarios']['shock']
        self.assertEqual(out['projected_pnl_dollars'],0);self.assertEqual(out['pct_of_gross_exposure'],0)

    def test_unknown_shock_stays_unmodeled_after_aggregation_repair(self):
        s,p=fixture();s['positions'].append({**s['positions'][0],'sector':'Unknown'})
        result=evaluate(s,p,SCENARIO)['historical_scenarios']['shock']
        self.assertEqual(len(result['per_position']),2);self.assertEqual(result['unmodeled_symbols'],['AAA'])
        self.assertIsNone(result['projected_pnl_dollars']);self.assertIsNone(result['pct_of_gross_exposure'])

    def test_native_adapter_does_not_request_coerced_or_malformed_instruments(self):
        for positions in (True,'AAA',[None],[{'symbol':True}],[{'symbol':'AAA','multiplier':True}]):
            store=Store();store.docs['portfolio/snapshot.json']['positions']=positions
            _,_,env=load('portfolio-risk',store)
            env['batch_fetch_bars']=lambda *a:self.fail('Provider work for malformed positions')
            response=env['_run_private']({},None)
            self.assertTrue(json.loads(response['body'])['success'])
            self.assertEqual(store.docs['portfolio/risk.json']['status'],'INCOMPLETE')
            self.assertEqual(store.docs['portfolio/risk.json']['schema_version'],'2.0.2')

    def test_native_mixed_rows_preserve_input_and_complete_publication(self):
        store=Store();store.docs['portfolio/snapshot.json']=fixture()[0]
        store.docs['portfolio/snapshot.json']['positions'].append(None)
        _,_,env=load('portfolio-risk',store);calls=[]
        env['batch_fetch_bars']=lambda symbols,days:(calls.append((symbols,days)) or {})
        response=env['_run_private']({},None)
        self.assertTrue(json.loads(response['body'])['success'])
        self.assertEqual(store.docs['portfolio/risk.json']['n_positions'],2)
        self.assertEqual(len(calls),1)
        self.assertIsNone(store.docs['portfolio/snapshot.json']['positions'][1])


if __name__=='__main__':unittest.main()
