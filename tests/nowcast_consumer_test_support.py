"""Exercise actual consuming code with unqualified research; never invoke consumers."""
from pathlib import Path
from datetime import datetime,timezone,timedelta
import ast,io,json,sys,textwrap,types,unittest
ROOT=Path(__file__).resolve().parents[1]
sys.path[:0]=[str(ROOT/'aws/shared'),str(ROOT/'aws/lambdas/justhodl-macro-nowcast/tests')]
from nowcast_fixture import packet,STAMP
import nowcast_research as adapter
CANARY={'regime':'SLOWING','normalized_score':99,'recession_probability':99,'confidence':.7,'calls_eligible':True}
def source(fn):return (ROOT/'aws/lambdas'/fn/'source/lambda_function.py').read_text(encoding='utf-8')
def actual(fn,name,ns):
    node=next(n for n in ast.parse(source(fn)).body if isinstance(n,ast.FunctionDef) and n.name==name)
    exec(compile(ast.Module(body=[node],type_ignores=[]),'actual-nowcast-consumer','exec'),ns);return ns[name]
class Client:
    def get_object(self,**kw):return {'Body':io.BytesIO(json.dumps(CANARY).encode()),'LastModified':datetime.now(timezone.utc)}
class Boundaries(unittest.TestCase):
    def test_alpha_reads_stored_research_without_invoke_or_invented_confidence(self):
        f=actual('justhodl-alpha-confluence','get_current_regime',{'_s3j':lambda key:CANARY})
        self.assertEqual(f(),(None,None))
        f=actual('justhodl-alpha-confluence','apply_regime_adjustment',{'REGIME_SECTOR_MAP':{'SLOWING':{'Tech':-20}}})
        self.assertEqual(f(80,'Tech',None),(80,0))

    def test_activity_cannot_subtract_differently_scaled_indices(self):
        sys.path.insert(0,str(ROOT/'aws/lambdas/justhodl-activity-nowcast/tests'))
        from activity_fixture import fixture,store
        client,inputs,_,_=fixture()
        p=store.compile_output(inputs,store.reader(client,'b'))
        self.assertFalse(p['divergence']['available']);self.assertIsNone(p['divergence']['gap'])
        self.assertIsNone(p['regime']);self.assertIsNone(p['activity_index'])
        self.assertFalse(p['calls_eligible'])

    def test_null_or_unknown_sector_regime_never_becomes_muddle(self):
        sys.path.insert(0,str(ROOT/'aws/shared/tests'))
        from test_sector_research import fixtures
        import sector_research_model,sector_tilt_model
        source,sources,_=fixtures();native=sector_research_model.build(source,sources,source['generated_at'])
        native['replay']={'manifest_key':'data/sector-research/runs/'+'a'*64+'.json','output_sha256':sector_research_model.sha(sector_research_model.encoded(native))}
        out=sector_tilt_model.build(native,source['generated_at'],{'data/macro-nowcast.json':CANARY},{})
        self.assertIsNone(out['regime']);self.assertEqual(out['summary']['n_unavailable'],11);self.assertEqual(out['summary']['n_neutral'],0)
        self.assertEqual(out['summary']['top_buy_opportunities'],[])
        for card in out['tilts']:
            self.assertIsNone(card['regime_tilt_score']);self.assertEqual(card['implication'],'WAIT');self.assertFalse(card['calls_eligible'])

    def test_actual_reads_drop_legacy_score_and_do_not_change_other_packets(self):
        f=actual('justhodl-quantum-desk','read_source',{'LOCAL_DIR':None,'json':json,'s3':Client(),'BUCKET':'b','_now':lambda:datetime.now(timezone.utc)})
        self.assertIsNone(f('macro',{'key':'data/macro-nowcast.json','max_age_h':30})[0]['regime'])
        f=actual('justhodl-wl-fusion','gj',{'json':json,'S3':Client(),'BUCKET':'b'})
        self.assertIsNone(f('data/macro-nowcast.json')['normalized_score']);self.assertEqual(f('data/other.json'),CANARY)
        for fn in ('justhodl-engine-trust','justhodl-signal-harvester'):
            f=actual(fn,'current_regime',{'_read':lambda key:CANARY if key=='data/macro-nowcast.json' else {}})
            self.assertIsNone(f())

    def test_allocator_and_ranker_assignments_have_no_macro_authority(self):
        for fn,name,reader in (('justhodl-master-allocator','mn','read_json'),('justhodl-master-ranker','nowcast','fetch_json')):
            tree=ast.parse(source(fn));node=next(n for n in ast.walk(tree) if isinstance(n,ast.Assign) and any(isinstance(t,ast.Name) and t.id==name for t in n.targets))
            ns={reader:lambda *a,**kw:CANARY};exec(compile(ast.Module(body=[node],type_ignores=[]),'actual-assignment','exec'),ns)
            for field in ('regime','normalized_score','score','recession_probability'):self.assertIsNone(ns[name][field])

    def test_interpreter_abstains_before_ai_notification_or_state_reads(self):
        writes=[];reads=[]
        def read(key):reads.append(key);return CANARY
        ns={'time':types.SimpleNamespace(time=lambda:1),'datetime':datetime,'timezone':timezone,'json':json,'load_s3_json':read,
            'S3_KEY_DIV':'data/divergence-v2.json','S3_KEY_NOW':'data/macro-nowcast.json','S3_KEY_OUT':'data/divergence-interpreted.json',
            'BUCKET':'b','S3':types.SimpleNamespace(put_object=lambda **kw:writes.append(kw))}
        p=json.loads(actual('justhodl-divergence-interpreter','lambda_handler',ns)({},None)['body'])
        self.assertEqual(p['status'],'ABSTAIN_UNQUALIFIED_MACRO');self.assertIsNone(p['regime']);self.assertEqual(p['paid_ai_calls'],0)
        self.assertEqual(reads,['data/divergence-v2.json','data/macro-nowcast.json']);self.assertEqual(len(writes),1)

    def test_native_context_requires_hash_current_clock_and_no_authority(self):
        _,_,p=packet();at=datetime.fromisoformat(STAMP)
        self.assertTrue(adapter.context(p,at)['available']);self.assertFalse(adapter.context(p,at+timedelta(hours=27))['available'])
        self.assertFalse(adapter.context(p,at-timedelta(seconds=1))['available'])
        for bad in (CANARY,{},None,dict(p,calls_eligible=True)):
            self.assertIsNone(adapter.qualified_score(bad));self.assertFalse(adapter.context(bad,at)['available'])
        p['research_index']['current']['value']=999;self.assertFalse(adapter.context(p,at)['available'])
        self.assertIs(adapter.guard('data/other.json',CANARY),CANARY)

if __name__=='__main__':unittest.main(verbosity=2)
