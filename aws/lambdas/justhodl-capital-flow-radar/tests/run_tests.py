import copy
import importlib.util
import sys
import types
import unittest
from datetime import datetime,timezone,timedelta
from pathlib import Path
from unittest.mock import patch
with patch.dict(sys.modules,{'boto3':types.SimpleNamespace(client=lambda *a,**k:None)}):
    spec=importlib.util.spec_from_file_location('radar_test',Path(__file__).resolve().parents[1]/'source/lambda_function.py')
    e=importlib.util.module_from_spec(spec);spec.loader.exec_module(e)
NOW=datetime.now(timezone.utc).date()
DATES=[(NOW-timedelta(days=i)).isoformat() for i in range(21)]
def row(ticker,value):
    return {'ticker':ticker,'methodology_version':'etf-dated-flows.v2','date_basis':'effective_date','unit':'USD','measure':'provider_fund_flow',
            'observation_date':DATES[0],'quality':{'status':'fresh'},'daily_flow_usd':value,'aum_usd':1000,
            'windows':{str(n):{'status':'complete_observed_window','n':n,'sum_usd':value*n,'dates':DATES[:n]} for n in (5,21)}}


class Flow(unittest.TestCase):
    def test_missing_member_never_zero_filled(self):
        result=e.group_measurement(['A','B'],{'A':row('A',5)},NOW)
        self.assertEqual(result['missing'],['B']);self.assertIsNone(result['flow_5obs_usd'])

    def test_zero_creation_is_not_steady_inflow(self):
        with patch.object(e,'COMPLEXES',{'Example':{'primary':'A','core':['A'],'bull':[],'bear':[]}}),patch.object(e,'SINGLE_STOCK_LEV',{}):
            out=e.build_radar({'methodology_version':'etf-dated-flows.v2','metrics':[row('A',0)]},NOW)
        self.assertEqual(out['complexes'][0]['regime'],'ZERO_NET_FLOW')
        self.assertIsNone(out['complexes'][0]['pump_probability'])
        self.assertEqual(out['pump_setups'],[])

    def test_mismatched_window_dates_do_not_sum(self):
        a,b=row('A',1),row('B',2);b['windows']['5']['dates']=DATES[1:6]
        result=e.group_measurement(['A','B'],{'A':a,'B':b},NOW)
        self.assertIsNone(result['flow_5obs_usd']);self.assertEqual(result['daily_flow_usd'],3)

    def test_stale_effective_date_overrules_fresh_flag(self):
        a=row('A',2);a['observation_date']=(NOW-timedelta(days=9)).isoformat()
        self.assertIsNone(e.group_measurement(['A'],{'A':a},NOW)['daily_flow_usd'])

    def test_core_actual_dollars_and_inverse_comparison_are_separate(self):
        members={'core':['A'],'bull':['B'],'bear':['C'],'primary':'A'}
        with patch.object(e,'COMPLEXES',{'one':members,'overlap':members}),patch.object(e,'SINGLE_STOCK_LEV',{}):
            out=e.build_radar({'methodology_version':'etf-dated-flows.v2','metrics':[row('A',1),row('B',3),row('C',2)]},NOW)
        self.assertEqual(out['complexes'][0]['flow_5obs_usd'],5)
        agg=out['unique_leveraged_aggregate']
        self.assertEqual(agg['bull_minus_bear_flow_5obs_usd'],5)
        self.assertEqual(agg['total_fund_flow_5obs_usd'],25)
        self.assertEqual(agg['bull']['required_count'],1)

    def test_duplicate_ticker_excluded_instead_of_overwritten(self):
        with patch.object(e,'COMPLEXES',{'one':{'core':['A'],'primary':'A'}}),patch.object(e,'SINGLE_STOCK_LEV',{}):
            out=e.build_radar({'methodology_version':'etf-dated-flows.v2','metrics':[row('A',1),row('A',2)]},NOW)
        self.assertEqual(out['quality']['duplicate_tickers_excluded'],['A'])
        self.assertIsNone(out['complexes'][0]['flow_5obs_usd'])

    def test_shorter_pace_compared_to_disjoint_prior_window(self):
        a=row('A',2);a['windows']['21']['sum_usd']=26 # last five 10, previous sixteen 16
        with patch.object(e,'COMPLEXES',{'one':{'core':['A'],'primary':'A'}}),patch.object(e,'SINGLE_STOCK_LEV',{}):
            out=e.build_radar({'methodology_version':'etf-dated-flows.v2','metrics':[a]},NOW)
        self.assertEqual(out['complexes'][0]['recent_minus_prior_pace_usd_per_observation'],1)

    def test_absent_inverse_suite_is_not_zero_flow(self):
        self.assertEqual(e.group_measurement([],{},NOW)['status'],'not_applicable')
        self.assertIsNone(e.group_measurement([],{},NOW)['flow_5obs_usd'])


if __name__=='__main__':unittest.main()
