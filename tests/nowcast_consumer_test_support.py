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
        f=actual('justhodl-activity-nowcast','divergence',{'s3':Client(),'S3_BUCKET':'b','json':json})
        p=f(-60);self.assertFalse(p['available']);self.assertIsNone(p['gap']);self.assertIsNone(p['monthly_regime'])

    def test_null_or_unknown_sector_regime_never_becomes_muddle(self):
        ns={'SECTOR_TILT_MATRIX':{'XLF':{'MUDDLE':1}}};normalize=actual('justhodl-sector-tilt','normalize_regime',ns)
        for value in (None,'','UNAVAILABLE','something new'):self.assertIsNone(normalize(value))
        ns.update(TILT_LABELS={0:'NEUTRAL'})
        card=actual('justhodl-sector-tilt','build_tilt_card',ns)('XLF',None,{'rs_vs_spy':{'20':8}})
        self.assertIsNone(card['regime_tilt_score']);self.assertEqual(card['implication'],'WAIT');self.assertFalse(card['calls_eligible'])
        summary=actual('justhodl-sector-tilt','build_summary',{}) ([card])
        self.assertEqual(summary['n_unavailable'],1);self.assertEqual(summary['n_neutral'],0);self.assertEqual(summary['top_buy_opportunities'],[])

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
