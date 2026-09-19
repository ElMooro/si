"""Production consumer blocks preserve descriptive exchange scope and abstention."""
from datetime import datetime, timezone, timedelta
import sys
from pathlib import Path
import unittest
ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(Path(__file__).parent))
from test_inflection_authority import functions
from test_global_liquidity_authority import block


class Tests(unittest.TestCase):
    def test_risk_cannot_turn_exchange_measurements_into_capital_flight_sizing(self):
        packet = {'calls_eligible': False, 'inflow_leaders': [], 'outflow_leaders': [
            {'region': 'Asia', 'country': 'Taiwan', 'conviction': 'CONFIRMED_OUTFLOW'}]*3}
        out = block('risk-regime','    # ── cross-border flow overlay (hot-money; capital-flight confirmation, not core score) ──\n',
            '    # ── systemic-stress overlay', {'_read':lambda key:packet,'score':40,'posture':{'size_mult':1},'all_tells':[]})
        self.assertIsNone(out['cross_border']); self.assertEqual(out['posture'], {'size_mult':1}); self.assertEqual(out['all_tells'], [])

    def test_accumulation_cannot_claim_unqualified_flow_corroboration(self):
        packet = {'calls_eligible':False,'all_countries':[{'country':'Taiwan','conviction':'CONFIRMED_INFLOW'}]}
        out = block('accumulation-radar','    # hot-money cross-corroboration: country -> conviction (so accumulation + foreign flow can agree)\n',
                    '    # short-interest map', {'_read':lambda key:packet})
        self.assertEqual(out['hm_conv'], {})

    def test_morning_narrative_cannot_promote_descriptive_feed(self):
        packet = {'calls_eligible':False,'inflow_leaders':[{'country':'Taiwan','conviction':'TWIN_ENGINE'}],
                  'outflow_leaders':[{'country':'Taiwan'}],'em_debt_flows':{'signal':'INFLOW'}}
        out = block('morning-intelligence','    # cross-border + cycle + leadership facts (hot-money + accumulation-radar)\n',
                    '    accum_bottoms =', {'data':{'hot_money':packet}})
        self.assertEqual(out['hm_in'], []); self.assertEqual(out['hm_out'], []); self.assertEqual(out['hotm'], {})

    def test_etf_context_keeps_original_and_observation_window_without_capital_signal(self):
        now = datetime(2026,9,19,8,tzinfo=timezone.utc); stamp=now.isoformat()
        p={'contract':'hot-money-research.v1','calls_eligible':False,'generated_at':stamp,'replay':{'manifest_key':'fixture'},
           'countries':{'taiwan':{'status':'LIVE','latest_bn':0,'sum_5obs_bn':12,'latest_day':'20260918',
             'quality':{'status':'fresh','acquired_at':stamp,'observation_date':'2026-09-18'},'latest':{'original':{'key':'fixture'}}}}}
        fn=functions('etf-global-desk',{'_hot_money_board'},{'datetime':datetime})['_hot_money_board']
        out=fn(p,now);self.assertEqual(out['latest_bn'],0);self.assertEqual(out['sum_5obs_bn'],12)
        self.assertIsNone(out['sum_5d_bn']);self.assertFalse(out['sizing_eligible']);self.assertEqual(out['original'],{'key':'fixture'})
        self.assertIsNone(fn(p,now+timedelta(days=2)));p['countries']['taiwan']['latest_bn']=True;self.assertIsNone(fn(p,now))


if __name__=='__main__':unittest.main()
