"""Exercise actual tail-read boundaries without account handlers or external services."""
from pathlib import Path
from datetime import datetime,timezone,timedelta
import ast,io,json,sys,textwrap,unittest
ROOT=Path(__file__).resolve().parents[1];sys.path[:0]=[str(ROOT/'aws/shared'),str(ROOT/'aws/lambdas/justhodl-tail-risk/tests')]
from native_tail_tests import packet,STAMP
import tail_research as adapter
CANARY={'generated_at':STAMP,'system_tail_gauge':99,'tail_regime':'STRESSED','tail_valuation':'CHEAP',
    'indices':[{'ticker':'SPY','tail_stress':99,'p_drop_10':.99}],'calls_eligible':True}
def source(fn):return (ROOT/'aws/lambdas'/fn/'source/lambda_function.py').read_text(encoding='utf-8')
def actual(fn,name,ns):
    node=next(n for n in ast.parse(source(fn)).body if isinstance(n,ast.FunctionDef) and n.name==name)
    exec(compile(ast.Module(body=[node],type_ignores=[]),'actual-tail-reader','exec'),ns);return ns[name]
class Client:
    def get_object(self,**kw):return {'Body':io.BytesIO(json.dumps(CANARY).encode()),'LastModified':datetime.now(timezone.utc)}
class Boundaries(unittest.TestCase):
    def test_cycle_and_implied_probability_actual_reads_drop_legacy_density(self):
        line=next(l for l in source('justhodl-cycle-clock').splitlines() if '"tail_research"' in l)
        ns={'load':lambda *a:CANARY};exec(textwrap.dedent(line),ns);self.assertIsNone(ns['tailrisk']['system_tail_gauge'])
        line=next(l for l in source('justhodl-implied-prob').splitlines() if '"tail_research"' in l)
        ns={'json':json,'S3':Client(),'BUCKET':'b'};exec(textwrap.dedent(line),ns);self.assertEqual(ns['tr']['indices'],[])
    def test_stress_strategist_and_quantum_actual_dynamic_readers(self):
        ns={'json':json,'s3':Client(),'S3_BUCKET':'b'};read=actual('justhodl-stress-index','_read_s3_json',ns)
        self.assertIsNone(read('data/tail-risk.json')['system_tail_gauge']);self.assertEqual(read('other.json'),CANARY)
        read=actual('justhodl-strategist','load',{'json':json,'S3':Client(),'BUCKET':'b','datetime':datetime,'timezone':timezone})
        self.assertIsNone(read('data/tail-risk.json')[1]['tail_regime'])
        read=actual('justhodl-quantum-desk','read_source',{'LOCAL_DIR':None,'json':json,'s3':Client(),'BUCKET':'b','_now':lambda:datetime.now(timezone.utc)})
        self.assertEqual(read('tail',{'key':'data/tail-risk.json','max_age_h':40})[0]['indices'],[])
    def test_both_katlin_feed_paths_apply_the_same_boundary(self):
        lines=[l for l in source('justhodl-katlin').splitlines() if 'tail_research' in l];self.assertEqual(len(lines),2)
        for line in lines:
            ns={'F':{},'name':'tail','key':'data/tail-risk.json','s3_json':lambda *a:CANARY,'WAR_ROOM_FEEDS':[('tail','data/tail-risk.json')]}
            exec(textwrap.dedent(line),ns);self.assertIsNone(ns['F']['tail']['tail_valuation'])
    def test_adapter_context_is_typed_hashed_bounded_and_never_a_vote(self):
        _,_,p=packet();at=datetime.fromisoformat(STAMP)
        self.assertTrue(adapter.context(p,at)['available']);self.assertFalse(adapter.context(p,at+timedelta(hours=27))['available'])
        self.assertFalse(adapter.context(p,at-timedelta(seconds=1))['available'])
        for value in (CANARY,None,{},dict(p,calls_eligible=True),dict(p,system_tail_gauge=99)):
            self.assertIsNone(adapter.qualified_score(value));self.assertFalse(adapter.context(value,at)['available']);self.assertEqual(adapter.decision_view(value)['indices'],[])
        p['indices'][0]['sample']['eligible_identity_rows']=999;self.assertFalse(adapter.context(p,at)['available'])
    def test_other_feeds_and_no_capital_authority(self):
        self.assertEqual(adapter.guard('data/other.json',CANARY),CANARY)
        view=adapter.guard('data/tail-risk.json',CANARY)
        self.assertIsNone(view['system_tail_gauge']);self.assertEqual(view['portfolio_action'],'WAIT')
        self.assertTrue(all(view[k] is False for k in adapter.PERMISSIONS))
def run():
    result=unittest.TextTestRunner(verbosity=2).run(unittest.defaultTestLoader.loadTestsFromTestCase(Boundaries))
    if not result.wasSuccessful():raise SystemExit(1)
if __name__=='__main__':run()
