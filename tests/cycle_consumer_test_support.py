"""Exercise actual Cycle Clock read boundaries without consumer invocations."""
from pathlib import Path
from datetime import datetime,timezone,timedelta
import ast,io,json,sys,textwrap,unittest
ROOT=Path(__file__).resolve().parents[1]
sys.path[:0]=[str(ROOT/'aws/shared'),str(ROOT/'aws/lambdas/justhodl-cycle-clock/tests')]
from cycle_fixture import packet,STAMP
import cycle_research as adapter
CANARY={'cycle':{'phase':'EARLY CYCLE'},'synthesis':{'score':99,'posture':'RISK-ON'},'verdict':'BUY','calls_eligible':True}
def source(fn,name='lambda_function.py'):return (ROOT/'aws/lambdas'/fn/'source'/name).read_text(encoding='utf-8')
def actual(fn,name,ns):
    node=next(n for n in ast.parse(source(fn)).body if isinstance(n,ast.FunctionDef) and n.name==name)
    exec(compile(ast.Module(body=[node],type_ignores=[]),'actual-cycle-reader','exec'),ns);return ns[name]
class Client:
    def get_object(self,**kw):return {'Body':io.BytesIO(json.dumps(CANARY).encode()),'LastModified':datetime.now(timezone.utc)}
class Boundaries(unittest.TestCase):
    def test_actual_industry_and_risk_assignments_drop_unqualified_phase(self):
        line=next(l for l in source('justhodl-industry-rotation').splitlines() if '_cc = ' in l)
        ns={'s3_json':lambda *a:CANARY};exec(textwrap.dedent(line),ns)
        self.assertIsNone(ns['_cc']['cycle']['phase']);self.assertIsNone(ns['_cc']['verdict'])
        tree=ast.parse(source('justhodl-khalid-risk','risk_engine.py'))
        node=next(n for n in ast.walk(tree) if isinstance(n,ast.Assign) and any(isinstance(t,ast.Name) and t.id=='cycle' for t in n.targets))
        ns={'mapping':lambda p:p if isinstance(p,dict) else {},'active':{'cycle_clock':CANARY}}
        exec(compile(ast.Module(body=[node],type_ignores=[]),'actual-risk-cycle','exec'),ns)
        self.assertIsNone(ns['cycle']['synthesis']['score']);self.assertEqual(ns['cycle']['synthesis']['posture'],'WAIT')
    def test_actual_dynamic_readers_cannot_scan_legacy_cycle_into_a_vote(self):
        read=actual('justhodl-strategist','load',{'json':json,'S3':Client(),'BUCKET':'b','datetime':datetime,'timezone':timezone})
        self.assertIsNone(read('data/cycle-clock.json')[1]['verdict'])
        read=actual('justhodl-quantum-desk','read_source',{'LOCAL_DIR':None,'json':json,'s3':Client(),'BUCKET':'b','_now':lambda:datetime.now(timezone.utc)})
        self.assertIsNone(read('cycle',{'key':'data/cycle-clock.json','max_age_h':30})[0]['phase'])
        read=actual('justhodl-wl-fusion','gj',{'json':json,'S3':Client(),'BUCKET':'b'})
        self.assertIsNone(read('data/cycle-clock.json')['verdict']);self.assertEqual(read('data/other.json'),CANARY)
    def test_fusion_adapter_and_both_registry_twins_exclude_scores(self):
        sys.path.insert(0,str(ROOT/'aws/lambdas/justhodl-engine-fusion/source'))
        from fusion_engine import adapt
        score,direction,detail=adapt({'adapter':'cycle'},CANARY,{})
        self.assertIsNone(score);self.assertIsNone(direction);self.assertFalse(detail['research_context']['available'])
        a=ROOT/'config/fusion-registry.v1.json';b=ROOT/'aws/lambdas/justhodl-engine-fusion/source/fusion-registry.v1.json'
        self.assertEqual(a.read_bytes(),b.read_bytes())
        spec=next(s for s in json.loads(a.read_bytes())['sources'] if s['id']=='cycle_clock')
        self.assertTrue(spec['exclude_from_scoring']);self.assertFalse(spec['independence_eligible'])
    def test_native_context_requires_hash_current_clock_and_no_authority(self):
        _,_,p=packet();at=datetime.fromisoformat(STAMP)
        self.assertTrue(adapter.context(p,at)['available'])
        self.assertFalse(adapter.context(p,at+timedelta(hours=27))['available'])
        self.assertFalse(adapter.context(p,at-timedelta(seconds=1))['available'])
        for bad in (CANARY,{},None,dict(p,calls_eligible=True)):
            self.assertIsNone(adapter.qualified_score(bad));self.assertFalse(adapter.context(bad,at)['available'])
        p['measurements']['INDPRO']['value']=999;self.assertFalse(adapter.context(p,at)['available'])
    def test_other_inputs_unchanged_and_wait_is_abstention(self):
        self.assertIs(adapter.guard('data/other.json',CANARY),CANARY)
        p=adapter.guard('data/cycle-clock.json',CANARY)
        self.assertEqual(p['portfolio_action'],'WAIT');self.assertTrue(all(p[k] is False for k in adapter.PERMISSIONS))
if __name__=='__main__':unittest.main(verbosity=2)
