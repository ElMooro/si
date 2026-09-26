from pathlib import Path
import ast,copy,json,sys,unittest
ROOT=Path(__file__).resolve().parents[4];SRC=Path(__file__).resolve().parents[1]/'source'
sys.path[:0]=[str(SRC),str(ROOT/'tests'),str(Path(__file__).parent)]
import bond_credit_store as store
import bond_credit as model
from test_bond_credit_candidate import fixture as candidate_fixture,AT
from test_cohort_native import Memory


def fixture():
    m=Memory();p=candidate_fixture();raw=store.encode(p);digest=store.sha(raw)
    output={'key':'data/credit-research/outputs/'+digest+'.json','sha256':digest,'bytes':len(raw)};m.data[output['key']]=raw;compilers={}
    for name,h in store.UPSTREAM.items():
        path=ROOT/'aws/shared/dealer_research_context.py' if name=='dealer_research_context' else ROOT/'aws/lambdas/justhodl-credit-stress/source'/(name+'.py')
        key='data/credit-research/compilers/'+h+'.py';m.data[key]=path.read_bytes();compilers[name]={'key':key,'sha256':h}
    run={'contract':'credit-native-replay.v1','generated_at':p['generated_at'],'output':output,'output_sha256':digest,'compilers':compilers}
    raw=store.encode(run);key='data/credit-research/runs/'+store.sha(raw)+'.json';m.data[key]=raw
    p['replay']={'manifest_key':key,'output_sha256':digest};m.data[store.SOURCE]=store.encode(p)
    return m,p


class Tests(unittest.TestCase):
    def test_whole_credit_binding_retention_and_typed_replay(self):
        m,p=fixture();out=store.collect(m,'bucket',AT);self.assertEqual(out['arithmetic_checks']['comparisons_checked'],4)
        self.assertEqual(store.replay(out,store.reader(m,'bucket')),{k:v for k,v in out.items() if k!='replay'})
        self.assertTrue(all(key.startswith(store.PREFIX) for key in m.writes));self.assertIs(out['upstream_binding']['credit_originals_replayed_here'],False)
        self.assertEqual((SRC/'bond_credit.py').read_bytes(),(ROOT/'aws/ops/checks/bond_credit_candidate.py').read_bytes())

    def test_bad_head_compiler_source_or_replay_cannot_publish_a_comparison(self):
        for kind in ('head','compiler','run'):
            m,p=fixture()
            if kind=='head':p['calls_eligible']=True;m.data[store.SOURCE]=store.encode(p)
            elif kind=='compiler':m.data[next(k for k in m.data if '/compilers/' in k)]=b'changed'
            else:m.data[p['replay']['manifest_key']]+=b' '
            with self.assertRaises(ValueError):store.collect(m,'bucket',AT)
            self.assertEqual(m.writes,[])
        m,p=fixture();out=store.collect(m,'bucket',AT);out['comparisons']['ccc_minus_bb']['value_bps']=False
        with self.assertRaises(ValueError):store.replay(out,store.reader(m,'bucket'))

    def test_expired_packet_is_a_retained_missing_result_not_fresh_zero(self):
        m,p=fixture();out=store.collect(m,'bucket','2026-09-29T19:00:00Z')
        self.assertEqual(out['arithmetic_checks']['comparisons_checked'],0)
        self.assertTrue(all(row['value_bps'] is None for row in out['comparisons'].values()))
        self.assertEqual(store.replay(out,store.reader(m,'bucket')),{k:v for k,v in out.items() if k!='replay'})

    def test_actual_legacy_field_projection_cannot_guess_units_or_fallback_to_old_micro(self):
        tree=ast.parse((SRC/'lambda_function.py').read_text(encoding='utf-8'))
        handler=next(n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name=='lambda_handler')
        nodes=[n for n in handler.body if isinstance(n,ast.Assign) and any(isinstance(t,ast.Name) and t.id in ('us_credit','EM') for t in n.targets)]
        for value in (None,0,2,948):
            scope={'credit_fields':{k+'_bps':value for k in model.PAIRS},'credit_research':{'contract':'fixture'},'credit_status':'fixture','micro':{'ccc_bb_bps':99999},'B':{'em_debt':{'flow_5d_usd':None}}}
            exec(compile(ast.Module(body=nodes,type_ignores=[]),'actual-credit-fields','exec'),scope)
            self.assertEqual(scope['us_credit']['ccc_minus_bb_bps'],value);self.assertEqual(scope['EM']['em_hy_minus_us_hy_bps'],value)
            self.assertEqual(scope['us_credit']['composite_regime'],'RESEARCH_ONLY');self.assertIs(scope['us_credit']['calls_eligible'],False)

if __name__=='__main__':unittest.main()
