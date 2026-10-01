"""Execute actual shipping input/scoring boundaries using isolated memory inputs."""
import ast,copy,hashlib,importlib.util,json,sys,unittest
from datetime import datetime,timedelta,timezone
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'aws/shared'))
import shipping_model_context as shipping
NOW=datetime.now(timezone.utc)


def packet():
    return {'generated_at':NOW.isoformat(),'ports':[{'name':'Example','country':'China','yoy_pct':999,
        'industry_exposure':{'industries':[{'industry':'Semiconductors','share_pct':0}]}}],
        'exporters':[{'country':'China','avg_vs_baseline_pct':999,'verdict':'BOOM','n_ports':2}],
        'calls_eligible':True,'sizing_eligible':True,'unknown_retained':{'all':[0,None,'original']}}


class Tests(unittest.TestCase):
    def test_all_parsed_fields_bound_without_mutation_or_self_promoted_permission(self):
        for p in (packet(),{**packet(),'contract':'portwatch-preserved-calculation.v1','portfolio_action':'WAIT',**dict.fromkeys(shipping.FLAGS,False)},None):
            old=copy.deepcopy(p);view=shipping.decision_view(p);self.assertEqual(p,old)
            self.assertEqual(view['ports'],[]);self.assertEqual(view['exporters'],[])
            self.assertTrue(all(view[k] is False for k in shipping.FLAGS))
            self.assertEqual(view['research_context']['qualified_investment_votes'],0)
            self.assertEqual(shipping.decision_view(view),view)
            forged=json.loads(json.dumps(view));forged['research_context']['parsed_input_sha256']='forged'
            self.assertNotEqual(shipping.decision_view(forged)['research_context']['parsed_input_sha256'],'forged')
        for invalid in (None,[],[0],False):
            ctx=shipping.context(invalid,NOW);self.assertFalse(ctx['source_object_valid'])
            self.assertEqual(ctx['parsed_input_sha256'],hashlib.sha256(json.dumps(invalid,separators=(',',':')).encode()).hexdigest())
        p=packet();ctx=shipping.context(p,NOW)
        self.assertEqual(ctx['parsed_input_sha256'],hashlib.sha256(json.dumps(p,sort_keys=True,separators=(',',':'),ensure_ascii=False,allow_nan=False).encode()).hexdigest())
        changed=copy.deepcopy(p);changed['unknown_retained']['all'][0]=1
        self.assertNotEqual(ctx['parsed_input_sha256'],shipping.context(changed,NOW)['parsed_input_sha256'])
        self.assertFalse(ctx['original_source_replay_performed_by_consumer'])

    def test_publication_clock_never_means_observation_or_forecast_freshness(self):
        p=packet();self.assertTrue(shipping.context(p,NOW)['publication_within_26h'])
        for stamp in (None,'bad','2026-09-27T05:00:00',(NOW+timedelta(seconds=1)).isoformat(),(NOW-timedelta(hours=27)).isoformat()):
            c=shipping.context({**p,'generated_at':stamp},NOW)
            self.assertFalse(c['publication_within_26h']);self.assertFalse(c['observation_freshness_verified'])
        p['invalid']=float('nan');self.assertIsNone(shipping.context(p,NOW)['parsed_input_sha256'])

    def native(self):
        path=ROOT/'aws/lambdas/justhodl-katlin/tests/run_tests.py'
        spec=importlib.util.spec_from_file_location('shipping_katlin_fixture',path)
        tests=importlib.util.module_from_spec(spec);spec.loader.exec_module(tests)
        saved={k:sys.modules.get(k) for k in ('boto3','botocore','botocore.exceptions','botocore.config')}
        try:mod=tests._load()
        finally:
            for k,v in saved.items():
                if v is None:sys.modules.pop(k,None)
                else:sys.modules[k]=v
        mod.log=lambda *a:None
        return mod,tests

    def feeds(self,mod,p):
        reads=[]
        def get(key,*args):
            reads.append(key)
            if key==shipping.SOURCE:return copy.deepcopy(p)
            if key=='data/industry-boom.json':return {'league':[{'industry':'Semiconductors','comp':{'rev_mean':16},'boom_score':80}]}
            return {}
        mod.s3_json=get
        return mod.load_feeds(),reads

    def test_actual_full_feed_loader_excludes_shipping_and_preserves_other_input(self):
        mod,_=self.native();p=packet();before=copy.deepcopy(p);F,reads=self.feeds(mod,p)
        self.assertEqual(p,before);self.assertEqual(reads.count(shipping.SOURCE),1)
        self.assertEqual(F['ports'],{});self.assertEqual(F['ports_countries'],{})
        self.assertEqual(F['boom']['Semiconductors']['comp']['rev_mean'],16)
        self.assertEqual(F['shipping_research_context']['parsed_input_sha256'],shipping.context(p)['parsed_input_sha256'])
        self.assertEqual(F['asof']['portwatch'],p['generated_at'])

    def test_actual_stock_and_country_etf_scoring_rejects_forged_prepared_shipping_maps(self):
        mod,fixture=self.native();F,_=self.feeds(mod,packet());before=copy.deepcopy(F)
        forged={**F,'ports':{'Semiconductors':{'yoy_pct':999,'ports':['Example']}},
                'ports_countries':{'China':{'yoy_pct':999,'n_ports':999}}}
        for symbol,kind in [('EXAMPLE','stock'),(mod.COUNTRY_ETF.get('China','MCHI'),'etf')]:
            args=(symbol,{}, {},F,1e9,kind,'Semiconductors','China','SMH')
            base=mod.catalyst_block(*args);other=mod.catalyst_block(*args[:3],forged,*args[4:])
            self.assertEqual(base,other);self.assertNotIn('"kind": "ports"',json.dumps(other))
            if kind=='stock':self.assertIn('industry_boom',json.dumps(other))
        self.assertEqual(F,before)
        authority={'risk_gate':fixture._gate(),'khalid_risk':fixture._auth(cap=50),'bond_warroom':fixture._funding()}
        self.assertEqual(mod.war_room(authority)['exposure_cap_pct'],50)

    def test_whole_predecessor_and_unrelated_functions_remain_identical(self):
        source=ROOT/'aws/lambdas/justhodl-katlin/source/lambda_function.py'
        previous=ROOT/'tests/fixtures/pre-shipping-qualification-katlin.py.txt'
        old={n.name:n for n in ast.parse(previous.read_text(encoding='utf-8')).body if isinstance(n,(ast.FunctionDef,ast.ClassDef))}
        new={n.name:n for n in ast.parse(source.read_text(encoding='utf-8')).body if isinstance(n,(ast.FunctionDef,ast.ClassDef))}
        for name,node in old.items():
            if name == 'war_room':
                # Funding and optional-vote repairs have native matrices; bind this
                # exception to the exact reviewed function, not arbitrary edits.
                self.assertEqual(hashlib.sha256(ast.dump(new[name]).encode()).hexdigest(), '041c38162238b5bdab303d3e59f378d946dc3a50f6dc7baef1f6c3bc03ce7b0a')
                continue
            if name in ('run_backtest', 'validation_summary'):
                from katlin_oos_test_support import assert_oos_only_change
                assert_oos_only_change(self, node, new[name])
                continue
            if name in ('s3_json','s3_json_quiet'):
                guarded=__import__('copy').deepcopy(new[name])
                expected=ast.parse('if key == "data/volatility-squeeze.json":\n    return _volatility_research_abstention()').body[0]
                self.assertEqual(ast.dump(guarded.body.pop(0)),ast.dump(expected))
                self.assertEqual(ast.dump(node),ast.dump(guarded),name)
                continue
            if name not in ('load_feeds','catalyst_block','_run_handler'):
                self.assertEqual(ast.dump(node),ast.dump(new[name]),name)
        self.assertEqual(hashlib.sha256(previous.read_bytes()).hexdigest(),'847b84fca0869ab3c73858702c40d746a60650d31c12aa9d62c8585320a2c029')


if __name__=='__main__':unittest.main(verbosity=2)
