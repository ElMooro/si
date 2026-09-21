"""Run actual consumer reads against unqualified canaries; no AWS or consumers invoked."""
from pathlib import Path
from datetime import datetime,timezone,timedelta
import ast,io,json,sys,types,unittest
ROOT=Path(__file__).resolve().parents[1];sys.path[:0]=[str(ROOT/'aws/shared'),str(ROOT/'aws/shared/tests')]
import sector_research as guard
from test_sector_research import fixtures,STAMP
import sector_research_model as model
CANARY={'market_breadth':'BROAD_LEADERSHIP','leaders':['XLK'],'sectors':[{'symbol':'XLK','ticker':'XLK','name':'Technology','regime':'LEADER','momentum_quintile':4,'rotation_score':99,'conviction':99,'posture':'OVERWEIGHT','quadrant':'Leading'}],'tilts':[{'ticker':'XLK','alignment':'MISALIGNED','implication':'BUY_OPPORTUNITY','urgency':'HIGH','regime_tilt_score':3}], 'rotation_alerts':{'rotating_in':[{'sym':'XLK'}]},'calls_eligible':True}

def actual(fn,name,ns):
    tree=ast.parse((ROOT/'aws/lambdas'/fn/'source/lambda_function.py').read_text(encoding='utf-8'))
    node=next(n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name==name)
    exec(compile(ast.Module(body=[node],type_ignores=[]),'actual-sector-consumer','exec'),ns);return ns[name]

class Client:
    def get_object(self,**kw):return {'Body':io.BytesIO(json.dumps(CANARY).encode())}

class SectorBoundaries(unittest.TestCase):
    def test_actual_allocator_alert_and_logger_cannot_vote_from_unqualified_sector(self):
        for name in ('rule_sector_breadth','rule_sector_momentum'):
            scores={};evidence=[]
            def add(*args):self.fail('Unqualified sector input changed allocation')
            actual('justhodl-allocator',name,{'fs3':lambda _:CANARY,'add':add})(scores,evidence)
            self.assertEqual(scores,{});self.assertEqual(evidence,[])
        alerts=[];actual('justhodl-alert-router','check_sector_rotation',{'load_json':lambda _:CANARY})(alerts);self.assertEqual(alerts,[])
        self.assertEqual(actual('justhodl-wave-signal-logger','log_sector_breadth',{'fs3':lambda _:CANARY})(),[])
    def test_deal_scanner_rejects_both_legacy_sector_layers_and_returns_stable_shape(self):
        f=actual('justhodl-deal-scanner','load_sector_signal',{'json':json,'s3':Client(),'S3_BUCKET':'b'})
        self.assertEqual(f(),({},set(),{},{}))
        def fail(**kw):raise ValueError('missing')
        f=actual('justhodl-deal-scanner','load_sector_signal',{'json':json,'s3':types.SimpleNamespace(get_object=fail),'S3_BUCKET':'b'})
        self.assertEqual(f(),({},set(),{},{}))
    def test_ranker_and_morning_brief_actual_assignments_drop_tilt_prescriptions(self):
        for fn,name in (('justhodl-master-ranker','tilt'),('justhodl-morning-brief-tg','sector_tilt'),('justhodl-master-ranker','_sf')):
            tree=ast.parse((ROOT/'aws/lambdas'/fn/'source/lambda_function.py').read_text(encoding='utf-8'))
            node=next(n for n in ast.walk(tree) if isinstance(n,ast.Assign) and any(isinstance(t,ast.Name) and t.id==name for t in n.targets))
            ns={'fetch_json':lambda *a:CANARY};exec(compile(ast.Module(body=[node],type_ignores=[]),'actual-sector-assignment','exec'),ns)
            self.assertEqual(ns[name]['tilts'],[]);self.assertEqual(ns[name]['sectors'],[]);self.assertEqual(ns[name]['portfolio_action'],'WAIT')
    def test_sector_flow_projection_emits_no_default_underweight(self):
        writes=[];ns={'rj':lambda _:CANARY,'datetime':datetime,'timezone':timezone,'json':json,'SPDR_TO_GICS':{},'BUCKET':'b',
            'S3':types.SimpleNamespace(put_object=lambda **kw:writes.append(kw))}
        out=actual('justhodl-sector-flow-state','lambda_handler',ns)({},None)
        self.assertEqual(out['n'],0);self.assertEqual(out['ow'],[]);self.assertEqual(out['uw'],[])
        packet=json.loads(writes[0]['Body']);self.assertEqual(packet['portfolio_action'],'WAIT');self.assertFalse(packet['calls_eligible'])
    def test_context_verifies_body_clock_and_only_descriptive_authority(self):
        packet,sources,_=fixtures();p=model.build(packet,sources,STAMP);p['replay']={'manifest_key':'data/sector-research/runs/'+'a'*64+'.json','output_sha256':model.sha(model.encoded(p))};at=datetime.fromisoformat(STAMP)
        self.assertTrue(guard.context(p,at)['available']);self.assertFalse(guard.context(p,at+timedelta(hours=27))['available'])
        for bad in (None,{},CANARY,{**p,'calls_eligible':True}):self.assertFalse(guard.context(bad,at)['available'])
        p['sectors'][0]['name']='modified';self.assertFalse(guard.context(p,at)['available'])
        self.assertIs(guard.guard('data/unrelated.json',CANARY),CANARY)
        self.assertEqual(guard.guard('data/sector-flow-state.json',CANARY)['sectors'],[])

if __name__=='__main__':unittest.main(verbosity=2)
