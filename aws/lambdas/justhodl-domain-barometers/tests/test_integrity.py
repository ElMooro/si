from pathlib import Path
from datetime import datetime, timezone
import runpy
import json
import os
import subprocess
import sys
import types
import unittest
from unittest.mock import patch
import barometer_integrity as model


class Tests(unittest.TestCase):
    def setUp(self):
        self.requests=[]
        client=types.SimpleNamespace(put_object=lambda **kw:self.requests.append(kw))
        with patch.dict(sys.modules,{'boto3':types.SimpleNamespace(client=lambda *a,**k:client)}):
            self.env=runpy.run_path(str(Path(model.__file__).with_name('lambda_function.py')))
        self.globals=self.env['build_barometers'].__globals__
        self.globals['polarity']=lambda s,c:(1,'Synthetic increasing-is-favourable rule','synthetic_fixture')

    def build(self,rows,gate=None):
        return self.env['build_barometers'](rows,{r['symbol']:'MACRO' for r in rows},gate or {},{r['symbol']:'macro' for r in rows},{})['MACRO']

    def row(self,value,prior,**extra):
        return {'symbol':'SYNTHETIC','status':'LIVE','value':value,'prev':prior,'unit':'index_points','source':'fred_alias:SYNTHETIC',**extra}

    def test_actual_empty_calculation_no_undefined_constitution(self):
        out=self.env['build_barometers']([],{}, {}, {}, {})
        self.assertEqual(set(out),{'MACRO','LIQUIDITY','RISK'})
        self.assertTrue(all(v['score_0_100'] is None for v in out.values()))
        self.assertEqual(self.requests,[])

    def test_negative_and_zero_baselines_use_level_differences(self):
        for value,prior,expected in [(-2,-1,-1),(-1,-2,1),(2,0,2),(0,0,0)]:
            row=self.row(value,prior,chg_pct=100)
            out=self.build([row]);record=out['driver_comparisons'][0]
            self.assertEqual(record['change_value'],expected);self.assertIsNone(record['chg_pct'])
            self.assertEqual(record['change_unit'],'index_points')
            self.assertEqual(out['n_favourable'],int(expected>0));self.assertEqual(out['n_adverse'],int(expected<0))
            self.assertEqual(out['n_unchanged'],int(expected==0))

    def test_unchanged_is_not_adverse_or_a_directional_vote(self):
        out=self.build([self.row(5,5)])
        self.assertEqual(out['n_unchanged'],1);self.assertEqual(out['n_adverse'],0)
        self.assertEqual(out['coverage']['voting'],0);self.assertIsNone(out['breadth_component'])
        self.assertEqual(out['coverage']['not_voting']['unchanged'],1)

    def test_unqualified_native_market_comparison_cannot_become_a_macro_vote(self):
        row=self.row(110,100,source='yahoo:GC=F',contract_version='native-market-observation.v1',
                     chg_pct=10,comparison_eligible=False)
        self.assertEqual(model.change(row,{}),(None,'native_price_comparison_unqualified'))
        out=self.build([row]);self.assertEqual(out['n_directional_comparisons'],0)
        self.assertEqual(out['n_comparison_unavailable'],1)

    def test_missing_nonfinite_boolean_and_untyped_values_do_not_vote(self):
        for value in (None,True,'12',float('inf'),float('nan')):
            out=self.build([self.row(value,None,chg_pct=1)])
            self.assertEqual(out['n_comparison_unavailable'],1);self.assertEqual(out['n_directional_comparisons'],0)
        for bad in (True,'2',float('inf'),float('nan'),3,-3):
            out=self.build([],{'legs':{'growth':{'score':bad}}});self.assertIsNone(out['gate_component'])
        self.assertEqual(self.build([],{'legs':{'growth':{'score':0}}})['gate_component'],50)

    def test_yoy_change_is_percentage_points_and_unknown_ledger_never_fills_it(self):
        out=self.build([self.row(-1,-2,unit='% YoY')]);self.assertEqual(out['driver_comparisons'][0]['change_unit'],'percentage_points')
        self.assertEqual(model.change({'symbol':'x','value':10},{'x':9}),(None,'comparison_unavailable'))
        self.assertEqual(model.change({'value':10,'chg_pct':0},{}),(0,'provider_reported_percent'))

    def test_mixed_unit_magnitudes_do_not_create_best_worst_rankings(self):
        rows=[self.row(1000000,1),{**self.row(2,1),'symbol':'OTHER','unit':'percent'}]
        out=self.build(rows);self.assertEqual(len(out['driver_comparisons']),2)
        self.assertEqual(out['worst_movers'],[]);self.assertEqual(out['best_movers'],[])
        self.assertFalse(out['permissions']['sizing_eligible'])

    def test_aliases_vote_once_and_conflicting_vintages_abstain(self):
        a=self.row(2,1,asof='2026-09-01');b={**a,'symbol':'ALIAS'}
        out=self.build([a,b]);self.assertEqual(out['n_favourable'],1)
        self.assertEqual(out['excluded_provider_roots'],{'duplicate_provider_series':1})
        self.assertEqual(len(out['driver_comparisons']),2)
        out=self.build([a,{**b,'asof':'2026-08-01'}]);self.assertEqual(out['n_favourable'],0)
        self.assertEqual(out['excluded_provider_roots'],{'conflicting_alias_vintage_or_rule':2})
        out=self.build([{**a,'source':'unknown'}]);self.assertEqual(out['n_favourable'],0)
        self.assertEqual(out['excluded_provider_roots'],{'unidentified_provider_series':1})

    def test_missing_domains_produce_wait_not_fabricated_neutral(self):
        for missing in (None,'50',True,float('nan')):
            bar={'LIQUIDITY':{'score_0_100':missing},'RISK':{'score_0_100':60},'MACRO':{'score_0_100':70}}
            out=self.env['predict'](bar,{'sizing_multiplier':1.2},{'l1_nowcast':{'class_prior':{'gold':float('inf')}}})
            self.assertTrue(all(r['direction']=='WAIT' and r['score'] is None and r['conviction'] is None for r in out['asset_classes'].values()))
            self.assertIsNone(out['regime_context']['sizing_multiplier'])
        bar={d:{'score_0_100':60} for d in ('LIQUIDITY','RISK','MACRO')}
        out=self.env['predict'](bar,{},{});self.assertTrue(all(r['status']=='unvalidated_rule_mapping' for r in out['asset_classes'].values()))

    def test_existing_ledger_is_not_rewritten_with_collection_dates(self):
        self.globals['gj']=lambda *a,**k:{'updated_at':'2026-09-01T00:00:00Z','observations':[{'date':'2026-09-01','values':{'SYNTHETIC':2}}]}
        out=self.env['update_ledger']({},[],datetime.now(timezone.utc))
        self.assertEqual(self.requests,[]);self.assertEqual(out['n_observations'],1)
        self.assertEqual(out['history_updated_at'],'2026-09-01T00:00:00Z');self.assertFalse(out['out_of_sample_validated'])

    def test_collection_clock_never_claims_observation_freshness(self):
        q=model.publication_quality({'bad':{},'future':{'generated_at':'2027-01-01T00:00:00Z'}},datetime(2026,9,20,tzinfo=timezone.utc))
        self.assertEqual(q['source_collection_clocks']['future']['status'],'future_clock')
        self.assertFalse(q['observation_freshness_verified']);self.assertFalse(q['independence_validated'])

    def test_actual_handler_publishes_typed_monitor_without_rewriting_ledger(self):
        docs={'data/brain.json':{'notes':[{'id':'synthetic-note','cat':'macro','text':'Synthetic growth observation'}]},
              'data/tradingview.json':{'generated_at':'2026-09-20T00:00:00Z','symbols':[self.row(-2,-1)]}}
        self.globals['gj']=lambda key,default=None:docs.get(key,default)
        self.env['lambda_handler']({},None)
        self.assertEqual([v['Key'] for v in self.requests],['data/domain-barometers.json'])
        out=json.loads(self.requests[0]['Body'])
        self.assertEqual(out['contract'],model.CONTRACT);self.assertFalse(out['permissions']['sizing_eligible'])
        self.assertEqual(out['quality']['status'],'unvalidated_monitor')
        self.assertEqual(len(out['symbols']),1)
        self.assertTrue(all(v['direction']=='WAIT' for v in out['predictions']['asset_classes'].values()))

    def test_classifier_ties_reproduce_across_python_hash_seeds(self):
        code='''
import json,runpy,sys,types
from pathlib import Path
p=Path(sys.argv[1]);sys.path[:0]=[str(p.parent),str(p.parents[3]/'shared')]
sys.modules['boto3']=types.SimpleNamespace(client=lambda *a,**k:None)
e=runpy.run_path(str(p));g=e['classify'].__globals__;g['ANCHORS']={'a':'MACRO','b':'RISK'}
brain={'notes':[{'id':'a','text':'[TV:FRED:AAA] synthetic output'}, {'id':'b','text':'[TV:FRED:BBB] synthetic stress'},
 {'id':'c','text':'[TV:FRED:TTT] FRED:AAA FRED:BBB synthetic comparison'}]}
rows=[{'symbol':s,'category':'macro'} for s in ('AAA','BBB','TTT')]
out=e['classify'](brain,rows);assert out[0]['TTT']=='MACRO' and out[1]['TTT']=='T3'
print(json.dumps(out,sort_keys=True))
'''
        outputs=[subprocess.check_output([sys.executable,'-c',code,str(Path(model.__file__).with_name('lambda_function.py'))],
                   env={**os.environ,'PYTHONHASHSEED':seed},text=True) for seed in ('1','2','17','999')]
        self.assertEqual(len(set(outputs)),1)


if __name__=='__main__':unittest.main()
