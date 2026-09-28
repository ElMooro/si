"""Synthetic consumer boundaries: no native services, messages or private data."""
from pathlib import Path
from unittest.mock import patch
from io import BytesIO
import ast,json,sys,unittest
ROOT=Path(__file__).resolve().parents[2]
FUNCTIONS={'compound-aggregator':['load_packet'],'options-confluence':['_read'],'master-ranker':['fetch_json'],
    'katlin':['s3_json','s3_json_quiet'],'ai-infra-stack':['_read'],'theme-second-wave':['_read']}
KEY='data/volatility-squeeze.json'


def extract(name,names,env):
    tree=ast.parse((ROOT/'aws/lambdas'/('justhodl-'+name)/'source/lambda_function.py').read_bytes())
    nodes=[n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name in [*names,'_volatility_research_abstention']]
    exec(compile(ast.Module(body=nodes,type_ignores=[]),'<isolated consumer>','exec'),env);return env


class Forbidden:
    def __getattr__(self,key):raise AssertionError('No storage or service request permitted')


class Tests(unittest.TestCase):
    def test_six_consumers_reject_direct_legacy_and_forged_tiers_before_storage(self):
        for name,functions in FUNCTIONS.items():
            with self.subTest(name=name):
                ns=extract(name,functions,{'S3':Forbidden(),'s3':Forbidden(),'_FEED_HEALTH':[]})
                for fn in functions:
                    out=ns[fn](KEY);self.assertEqual(out['investment_votes'],0)
                    self.assertEqual(out['basis'],'price-compression-abstention.v1');self.assertFalse(out['calls_eligible'])
                    for field in ('all_qualifying','tier_s','tier_a','squeezes','top'):self.assertNotIn(field,out)
                if name=='master-ranker':self.assertFalse(ns['_FEED_HEALTH'][0]['used'])
    def test_compound_normalizer_cannot_bypass_reader(self):
        ns=extract('compound-aggregator',['load_feed'],{'S3':Forbidden()})
        self.assertEqual(ns['load_feed'](KEY,'summary.top_25_overall','symbol'),[])
    def test_compound_actual_aggregation_excludes_price_and_previous_comparison(self):
        sys.path[:0]=[str(ROOT/'aws/shared/tests'),str(ROOT/'aws/shared')]
        from test_holdings_derived_boundary import load,Storage
        from holdings_derived_boundary import BASIS
        m=load('justhodl-compound-aggregator')
        for score in (10**12,-10**12):
            forged={'calls_eligible':True,'forecast_qualified':True,'summary':{'top_25_overall':[{'symbol':'FAKE','score':score,'tier':'TIER_S_EXCEPTIONAL'}]}}
            db=Storage({KEY:forged,'data/nobrainers.json':{'summary':{'top_25_overall':[{'ticker':'KO','score':40}]}},
                'data/insider-clusters.json':{'clusters':[{'ticker':'KO','score':60}]},
                'data/compound-history.json':{'days':[{'d':'2026-01-01','scores':{'KO':10**12},'score_basis':BASIS,'activist_boundary':'ownership-feed-abstention.v1'}]}})
            with patch.object(m,'S3',db),patch.object(m,'emit_alerts',side_effect=AssertionError('No notification')):
                m.lambda_handler({'suppress_alerts':True},None)
            p=db.writes[m.S3_KEY];self.assertEqual([r['symbol'] for r in p['compound']],['KO'])
            self.assertEqual(p['compound'][0]['n_systems'],2);self.assertNotIn('pctile_90d_self',p['compound'][0])
            self.assertEqual(p['feed_stats']['vol_squeeze'],0);self.assertEqual(p['volatility_research_exclusion']['investment_votes'],0)
            hist=db.writes['data/compound-history.json']['days'];self.assertEqual(hist[0]['scores']['KO'],10**12)
            self.assertEqual(hist[-1]['volatility_boundary'],'price-compression-abstention.v1')
    def test_master_index_rejects_stored_indirect_price_votes(self):
        sys.path[:0]=[str(ROOT/'aws/shared/tests'),str(ROOT/'aws/shared')]
        from test_holdings_derived_boundary import load
        from holdings_derived_boundary import BASIS
        m=load('justhodl-master-ranker');m.engine_trust=None
        compound={'holdings_exclusions':{'basis':BASIS},'compound':[{'symbol':'FAKE','compound_score':150,
            'n_systems':2,'systems':['insiders','vol_squeeze'],'scores':{'insiders':40,'vol_squeeze':60}}]}
        options={'multi_engine_confluence':[{'ticker':'FAKE','score':999,'n_engines':99,'posture':'COILED'}]}
        inputs={'data/compound-signals.json':compound,'data/options-confluence.json':options,
            'data/insider-clusters.json':{'clusters':[{'ticker':'KO','n_insiders':3,'total_value':100}]}}
        with patch.object(m,'fetch_json',side_effect=lambda key,**kw:inputs.get(key)):
            idx,_=m.build_ticker_index()
        self.assertEqual(set(idx),{'KO'});self.assertEqual(set(idx['KO']),{'insider'})


if __name__=='__main__':unittest.main()
