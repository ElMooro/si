"""No target, false confidence, or streaming alert may escape research qualification."""
from pathlib import Path
from copy import deepcopy
from datetime import datetime,timezone
from types import SimpleNamespace
import ast,hashlib,io,json,sys,unittest
ROOT=Path(__file__).resolve().parents[1]
sys.path[:0]=[str(ROOT/'aws/shared'),str(ROOT/'aws/ops/checks')]
import master_allocation_authority as gate
from public_brain_projection import sanitize_public
from release_package_evidence import shared_imports

PACKET={'as_of':'2026-09-26T12:20:00Z','posture':'RISK_ON','confidence':99,'target_allocation':{'stocks':100},
    'benchmark':{'stocks':60},'deltas_from_benchmark':{'stocks':40},'active_risk_bps':800,
    'signals_used':{'x':{'value':100,'intensity':1}},'best_asset':{'winner':{'asset':'SPY'},'ranked':[{'asset':'SPY','score':10}]},
    'sizing_eligible':True,'decision_qualification':{'status':'qualified'},'calls_eligible':True}


def functions(engine,names,scope):
    p=ROOT/f'aws/lambdas/justhodl-{engine}/source/lambda_function.py'
    nodes=[n for n in ast.parse(p.read_text(encoding='utf-8')).body if isinstance(n,ast.FunctionDef) and n.name in names]
    assert len(nodes)==len(names)
    exec(compile(ast.Module(body=nodes,type_ignores=[]),engine,'exec'),scope)
    return scope


class Tests(unittest.TestCase):
    def test_complete_calculation_is_preserved_but_cannot_self_qualify(self):
        original=deepcopy(PACKET);out=gate.project(PACKET)
        self.assertEqual(PACKET,original);self.assertEqual(out['unqualified_projection'],original)
        self.assertIsNot(out['unqualified_projection'],PACKET)
        for source in (None,[],{},PACKET,out,{**PACKET,'contract':'trusted-v99'}):
            view=gate.decision_view(source)
            self.assertTrue(all(view[k] is False for k in gate.FLAGS))
            self.assertIsNone(view['target_allocation']);self.assertIsNone(view['confidence'])
            self.assertIsNone(view['best_asset']['winner']);self.assertEqual(view['posture'],'WAIT')
            self.assertIsNone(gate.execution_packet(source)['target'])
        self.assertIs(gate.guard('data/other.json',PACKET),PACKET)

    def publisher(self,fail=False):
        writes=[]
        def ssm(**kw):
            if fail:raise RuntimeError('SSM write unavailable')
            writes.append(('ssm',json.loads(kw['Value'])))
        scope=functions('master-allocator',{'publish_research'},dict(json=json,sanitize_public=sanitize_public,
            OUT_KEY=gate.CURRENT,S3_BUCKET='fixture',TARGET_PARAM='/fixture/target',
            ssm=SimpleNamespace(put_parameter=ssm),s3=SimpleNamespace(put_object=lambda **kw:writes.append(('s3',json.loads(kw['Body']))))))
        return scope,writes

    def test_actual_publisher_clears_target_before_publishing_complete_research(self):
        scope,writes=self.publisher();out=scope['publish_research'](deepcopy(PACKET))
        self.assertEqual([r[0] for r in writes],['ssm','s3'])
        self.assertIsNone(writes[0][1]['target']);self.assertFalse(writes[0][1]['execution_eligible'])
        self.assertEqual(writes[1][1]['unqualified_projection']['target_allocation'],{'stocks':100})
        self.assertEqual(out['held_positions_action'],'NO_INSTRUCTION')

    def test_failed_machine_boundary_write_is_not_swallowed_or_published(self):
        scope,writes=self.publisher(True)
        with self.assertRaisesRegex(RuntimeError,'SSM'):scope['publish_research'](deepcopy(PACKET))
        self.assertEqual(writes,[])

    def test_actual_handler_response_cannot_leak_its_old_heuristic_confidence(self):
        scope,writes=self.publisher()
        assets=('us_equity','intl_dev_eq','em_equity','ust_long','ust_short','gold','cash','btc')
        scope.update(time=SimpleNamespace(time=lambda:0),datetime=datetime,timezone=timezone,ASSETS=assets,
            BENCHMARK={a:100/len(assets) for a in assets},ASSET_LABELS={a:a for a in assets},
            POSITION_LIMITS={a:(0,100) for a in assets},MAX_ACTIVE_BPS=800,TILT_MAGNITUDES={},
            gather_signals=lambda:{},load_ic_weights=lambda:{},
            aggregate_tilts=lambda *args:({a:0 for a in assets},{a:[] for a in assets},0),
            compass_bridge=lambda tilts,contributions:(tilts,{}),apply_limits_and_renormalise=lambda x:x,
            enforce_risk_budget=lambda target,*args:(target,False,0),_best_asset_now=lambda:{'winner':{'asset':'TEST'}})
        functions('master-allocator',{'lambda_handler'},scope)
        response=json.loads(scope['lambda_handler']({},None)['body'])
        self.assertEqual(response['posture'],'WAIT');self.assertIsNone(response['confidence'])
        self.assertIsNone(response['active_bps']);self.assertFalse(response['execution_eligible'])
        self.assertIsNone(writes[0][1]['target'])
        self.assertEqual(writes[1][1]['unqualified_projection']['confidence'],30)

    def test_actual_quantum_reader_excludes_old_and_new_allocator_votes(self):
        now=datetime(2026,9,26,13,tzinfo=timezone.utc)
        for packet in (PACKET,gate.project(PACKET)):
            scope=functions('quantum-desk',{'read_source'},dict(LOCAL_DIR=None,json=json,BUCKET='fixture',_now=lambda:now,
                s3=SimpleNamespace(get_object=lambda **kw:{'Body':io.BytesIO(json.dumps(packet).encode()),'LastModified':now})))
            out,meta=scope['read_source']('master_alloc',{'key':gate.CURRENT,'max_age_h':30})
            self.assertEqual(meta['status'],'research_only');self.assertIsNone(out['target_allocation'])
            self.assertIsNone(out['best_asset']['winner'])

    def test_actual_streaming_comparison_cannot_promote_or_alert_old_allocation(self):
        scope=functions('streaming-fanout',{'_extract_summary','_is_meaningful_delta'}, {})
        engine={'name':'master_alloc','summary_fields':['posture','confidence','active_risk_bps']}
        out=scope['_extract_summary'](engine,PACKET)
        self.assertNotIn('posture',out);self.assertFalse(out['research_context']['sizing_eligible'])
        for previous in (None,{'posture':'DEFENSIVE','active_risk_bps':0}):
            self.assertEqual(scope['_is_meaningful_delta'](engine,previous,PACKET),(False,'unqualified_master_allocation'))

    def test_all_four_actual_packages_bundle_the_boundary(self):
        for name in ('master-allocator','quantum-desk','regime-conditional-router','streaming-fanout'):
            source=ROOT/f'aws/lambdas/justhodl-{name}/source'
            self.assertIn('master_allocation_authority.py',[p.name for p in shared_imports(ROOT,list(source.glob('*.py')))])

    def test_complete_predecessors_are_preserved(self):
        for path,size,sha in (
            ('aws/lambdas/justhodl-master-allocator/tests/legacy_before_research_boundary.py.txt',36688,'81b1be8c30b2df6d733d4dc5192b55118cc3ccd8ff27b07af9cca4b025b87631'),
            ('tests/fixtures/legacy-master-allocator-before-research-boundary.html.txt',17747,'e38407de2f427699dfba89d99d139fe42adf10c05d7cc629371595de0e3d9ed8')):
            raw=(ROOT/path).read_bytes();self.assertEqual(len(raw),size);self.assertEqual(hashlib.sha256(raw).hexdigest(),sha)


if __name__=='__main__':unittest.main(verbosity=2)
