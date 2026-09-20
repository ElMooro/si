"""Exercise actual consumer boundaries and the full tail handler with synthetic data."""
from pathlib import Path
from datetime import datetime,timezone
from types import SimpleNamespace
from unittest.mock import patch
import ast,copy,json,sys,textwrap,unittest
ROOT=Path(__file__).resolve().parents[1]
sys.path[:0]=[str(ROOT/'aws/shared'),str(ROOT/'aws/lambdas/justhodl-vol-surface/tests')]
from volatility_fixtures import fixture,model,STAMP
from storage_volatility_tests import Memory
import volatility_research_store as store
import volatility_research as adapter

FUNCTIONS=('capitulation','hedge-planner','hedge-pnl','portfolio-analytics','kb-matcher','tail-hedge')
def source(fn):return (ROOT/'aws/lambdas'/('justhodl-'+fn)/'source/lambda_function.py').read_text(encoding='utf-8')

class ConsumerCases(unittest.TestCase):
    def setUp(self):
        inputs,bodies=fixture();s=Memory();s.objects.update(bodies)
        out=store.compile_output(inputs,store.reader(s,'test'))
        self.packet={**out,'replay':store.retain(s,'test',inputs,out)}
        self.at=datetime.fromisoformat(STAMP)

    def test_typed_source_dates_and_distinct_units(self):
        c=adapter.context(self.packet,self.at)
        self.assertEqual(len(c['measurements']),16)
        self.assertEqual(c['measurements']['SKEW']['unit'],'index_points')
        self.assertIn('observed 2026-09-17',adapter.describe(c))
        self.assertIn('no qualified trade',adapter.describe(c))

    def test_stale_future_legacy_and_self_qualified_packets_cannot_vote(self):
        for at in ('2026-09-22T03:00:00+00:00','2026-09-20T13:00:00+00:00'):
            self.assertFalse(adapter.context(self.packet,datetime.fromisoformat(at))['available'])
        self.assertFalse(adapter.context({'regime':'PANIC','term_structure':{'inverted':True}},self.at)['available'])
        p=copy.deepcopy(self.packet);p['calls_eligible']=True
        p['replay']['output_sha256']=model.sha(model.encoded({k:v for k,v in p.items() if k!='replay'}))
        self.assertFalse(adapter.context(p,self.at)['available']);self.assertIsNone(adapter.qualified_signal(p))

    def test_precision_or_source_reference_corruption_is_withheld(self):
        p=copy.deepcopy(self.packet);p['measurements']['VIX_30D']['value']+=1
        self.assertFalse(adapter.context(p,self.at)['available'])
        p=copy.deepcopy(self.packet);p['source_evidence'][0]['provider']='unknown'
        p['replay']['output_sha256']=model.sha(model.encoded({k:v for k,v in p.items() if k!='replay'}))
        self.assertFalse(adapter.context(p,self.at)['available'])

    def test_actual_cost_and_risk_blocks_do_not_default_missing_to_calm(self):
        for fn,marker,name in (('hedge-planner','    from volatility_research import cost_context','vol_cost_context'),
                               ('hedge-pnl','    from volatility_research import risk_context','massive_risk_context'),
                               ('portfolio-analytics','    from volatility_research import risk_context','risk_environment')):
            text=source(fn);start=text.index(marker);end=text.index('\n',text.index('    '+name+' =',start))+1
            for p in ({},self.packet,{'regime':'CALM','skew':{'pctile_252d':0}}):
                ns={'_vs':p,'_dg':{}}
                exec(textwrap.dedent(text[start:end]),ns)
                out=ns[name];self.assertEqual(out['hedge_cost_read'],'UNCLASSIFIED')
                self.assertIsNone(out['term_inverted']);self.assertFalse(out['sizing_eligible'])
                self.assertNotEqual(out.get('concentration_warning'),False)

    def test_capitulation_has_no_volatility_washout_vote(self):
        from extremes_native_test_support import synthesis_with
        out=synthesis_with('volatility',{'regime':'PANIC','composite_stress_score':100},self.at)
        self.assertEqual(out['measurements'],[]);self.assertIsNone(out['capitulation_score'])
        out=synthesis_with('volatility',self.packet,self.at)
        self.assertEqual(len(out['measurements']),16);self.assertEqual(out['decision']['eligible_votes'],0)
        direct={r['series_id'] for r in out['measurements'] if r['series_id'].startswith('CBOE:')}
        self.assertEqual(direct,{'CBOE:SKEW','CBOE:VVIX'})

    def test_kb_actual_state_has_no_unqualified_vix_rule_value(self):
        tree=ast.parse(source('kb-matcher'));node=next(n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name=='build_today_state')
        ns={'s3j':lambda *a,**kw:{},'fred_last':lambda *a,**kw:[]}
        exec(compile(ast.Module(body=[node],type_ignores=[]),'actual-kb-state','exec'),ns)
        out=ns['build_today_state']();self.assertNotIn('vix',out);self.assertIn('volatility_research',out)

    def test_tail_full_handler_with_and_without_loss_breach_handles_unknown_cost(self):
        tree=ast.parse(source('tail-hedge'));nodes=[]
        for node in tree.body:
            if isinstance(node,ast.FunctionDef):nodes.append(node)
            elif isinstance(node,ast.Assign):
                try:ast.literal_eval(node.value);nodes.append(node)
                except (ValueError,TypeError):pass
        class Clock(datetime):
            @classmethod
            def now(cls,tz=None):return datetime.fromisoformat(STAMP)
        for loss in (-25,-5,None):
            written={}
            fixtures={'data/firm-stress.json':{'summary':{'worst_scenario':'GFC','worst_loss_pct':loss,'n_scenarios':15},'loss_limits':{'soft_pct':-12,'hard_pct':-20}},
                'data/factor-risk.json':{'firm':{'net_market_beta':1,'var_99_1d_pct':2}},
                'data/firm-book.json':{'firm':{'net_exposure_pct':100}},
                'data/firm-risk-board.json':{'firm_posture':'RED','binding_constraint':{'dimension':'TAIL_STRESS'}},
                'data/eurodollar-plumbing.json':{'composite_score':20},'data/canary-grid.json':{'early_warning_level':20},
                'data/vol-surface.json':self.packet,
                'data/tail-hedge-history.json':{'snapshots':[{'date':'2026-09-19','annualised_carry_pct':2}]}}
            ns={'json':json,'time':SimpleNamespace(time=lambda:100),'datetime':Clock,'timezone':timezone,
                's3':SimpleNamespace(put_object=lambda **kw:written.update({kw['Key']:json.loads(kw['Body'])}))}
            exec(compile(ast.Module(body=nodes,type_ignores=[]),'actual-tail-handler','exec'),ns)
            ns['read_json']=lambda key:(fixtures.get(key,{}),self.at)
            result=ns['lambda_handler']({},None);self.assertEqual(result['statusCode'],200)
            out=written['data/tail-hedge.json']
            self.assertIsNone(out['hedge_sleeve']['annualised_carry_pct'])
            self.assertIsNone(out['cost_benefit']['insurance_ratio']);self.assertIsNone(out['deltas']['annualised_carry_pp'])
            self.assertEqual(out['regime']['stance'],'WAIT')
            self.assertNotIn('protection is cheap',out['cro_brief'])

    def test_every_consumer_packages_boundary(self):
        sys.path.insert(0,str(ROOT/'aws/ops/checks'));from release_package_evidence import shared_imports
        for fn in FUNCTIONS:
            paths=list((ROOT/'aws/lambdas'/('justhodl-'+fn)/'source').glob('*.py'))
            self.assertIn('volatility_research.py',[p.name for p in shared_imports(ROOT,paths)],fn)

    def test_http_and_validation_never_publish_or_resolve_credentials(self):
        node=next(n for n in ast.parse(source('vol-surface')).body if isinstance(n,ast.FunctionDef) and n.name=='lambda_handler')
        def forbidden(*a,**kw):raise AssertionError('Unexpected mutation or credential lookup')
        ns={'json':json,'CONTRACT':model.CONTRACT,'CURRENT':store.CURRENT,'Config':lambda **kw:None,
            'boto3':SimpleNamespace(client=lambda *a,**kw:None),'reader':lambda *a:lambda key:model.encoded(self.packet),'run':forbidden}
        exec(compile(ast.Module(body=[node],type_ignores=[]),'actual-volatility-handler','exec'),ns)
        self.assertEqual(ns['lambda_handler']({'httpMethod':'GET'})['statusCode'],200)
        ns['boto3']=SimpleNamespace(client=forbidden)
        self.assertEqual(ns['lambda_handler']({'validate_only':True})['statusCode'],200)

def run():
    if not unittest.TextTestRunner(verbosity=2).run(unittest.defaultTestLoader.loadTestsFromTestCase(ConsumerCases)).wasSuccessful():raise SystemExit(1)
if __name__=='__main__':run()
