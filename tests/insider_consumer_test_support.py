"""Actual insider consumer boundaries; no account reads, provider calls or writes."""
from pathlib import Path
from copy import deepcopy
from datetime import datetime,timezone
import ast,io,json,sys,textwrap,unittest
ROOT=Path(__file__).resolve().parents[1]
sys.path[:0]=[str(ROOT/'aws/shared'),str(ROOT/'aws/lambdas/justhodl-insider-aggregate/tests')]
from test_native_insider import packet,STAMP
import insider_research as adapter

def source(fn):return (ROOT/'aws/lambdas'/fn/'source/lambda_function.py').read_text(encoding='utf-8')
class Boundaries(unittest.TestCase):
    @classmethod
    def setUpClass(cls):cls.storage,cls.inputs,cls.packet=packet();cls.at=datetime.fromisoformat(STAMP)
    def test_native_sample_context_has_zero_authority(self):
        c=adapter.context(self.packet,self.at);self.assertTrue(c['available']);self.assertEqual(c['windows']['last_30d']['buy_count'],3)
        for k in adapter.PERMISSIONS:self.assertIs(c[k],False)
    def test_legacy_self_qualified_tampered_stale_and_future_are_refused(self):
        for p in ({'regime':'INSIDERS_ACCUMULATING','headline_ratio_30d_dollar':99},None,[],{}):self.assertFalse(adapter.context(p,self.at)['available'])
        p=deepcopy(self.packet);p['windows']['last_30d']['buy_count']=999;self.assertFalse(adapter.context(p,self.at)['available'])
        p=deepcopy(self.packet);p['calls_eligible']=True;self.assertFalse(adapter.context(p,self.at)['available'])
        for at in ('2026-09-18T00:00:00Z','2026-09-23T00:00:00Z'):self.assertFalse(adapter.context(self.packet,datetime.fromisoformat(at))['available'])
    def test_extremes_and_capitulation_cannot_get_insider_timing_vote(self):
        from extremes_native_test_support import synthesis_with
        bad={'regime':'INSIDERS_ACCUMULATING','headline_ratio_30d_dollar':99,'windows':{'last_30d':{'buy_sell_ratio_dollar':99}}}
        for engine in ('capitulation','market-extremes'):
            out=synthesis_with('insider',bad,self.at,engine)
            self.assertEqual(out['contexts']['insider'],{});self.assertIsNone(out['signal'])
            out=synthesis_with('insider',self.packet,self.at,engine)
            self.assertTrue(out['eligibility']['insider']['research_context_available']);self.assertEqual(out['decision']['eligible_votes'],0)

    def test_spinoff_cluster_cannot_add_unqualified_points(self):
        line=next(l for l in source('justhodl-spinoff-desk').splitlines() if 'insider_research' in l)
        ns={'json':json,'obj':{'Body':io.BytesIO(json.dumps({'notable_cluster_buys':[{'symbol':'TEST'}]}).encode())}}
        exec(textwrap.dedent(line),ns);self.assertEqual(ns['idoc']['notable_cluster_buys'],[])
    def test_best_ideas_actual_harvest_refuses_smart_money_confirmation(self):
        tree=ast.parse(source('justhodl-best-ideas'));node=next(n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name=='harvest')
        class Client:
            def get_object(self,**kw):return {'Body':io.BytesIO(json.dumps({'notable_cluster_buys':[{'symbol':'TEST','n_buyers':5}]}).encode())}
        def forbidden(*a):raise AssertionError('Unqualified insider ranking')
        ns={'json':json,'s3':Client(),'S3_BUCKET':'b','dig':forbidden};exec(compile(ast.Module(body=[node],type_ignores=[]),'actual-best-ideas','exec'),ns)
        got,status=ns['harvest'](('insider','Insider','data/insider-aggregate.json',['notable_cluster_buys'],'symbol',None,'SMART_MONEY','unused',None))
        self.assertEqual(got,{});self.assertEqual(status,'research_only_unqualified_insider_sample')
    def test_SEC_fleet_join_preserves_context_without_promoting_legacy_ratio(self):
        line=next(l for l in source('justhodl-insider-trades').splitlines() if 'insider_research' in l)
        ns={'out':{'sections':{}},'name':'aggregate','age':1,'stale':False,'key':'data/insider-aggregate.json','d':{'ratio':99,'regime':'PANIC'},'_slim_v2':lambda p:p}
        exec(textwrap.dedent(line),ns);self.assertIsNone(ns['out']['sections']['aggregate']['data']['regime'])
        self.assertEqual(adapter.guard('data/unrelated.json',{'zero':0}),{'zero':0})

def run():
    result=unittest.TextTestRunner(verbosity=2).run(unittest.defaultTestLoader.loadTestsFromTestCase(Boundaries))
    if not result.wasSuccessful():raise SystemExit(1)
if __name__=='__main__':run()
