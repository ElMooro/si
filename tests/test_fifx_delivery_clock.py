from pathlib import Path
from datetime import datetime,timedelta,timezone
from unittest.mock import patch
import copy,hashlib,importlib.util,importlib.machinery,io,json,sys,unittest
R=Path(__file__).resolve().parents[1];D=R/'tests/fixtures/fifx-delivery-clock'
sys.path[:0]=[str(R/'aws/lambdas/justhodl-fifx-vol-migration/source'),str(R/'aws/shared')]

def load(old=False):
    p=D/'predecessor.py.txt' if old else R/'aws/ops/checks/fifx_api_delivery.py'
    spec=importlib.util.spec_from_file_location('time_candidate',p,loader=importlib.machinery.SourceFileLoader('time_candidate',str(p)));m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m);return m

def fixture():
    import fifx_fred
    return {'source_url':fifx_fred.source_url('DGS10','2026-10-01'),'acquired_at':'2026-10-01T21:20:30Z','bytes':10,'sha256':'a'*64}, {'requested_start':'1988-01-01','requested_end':'2026-10-01','realtime_start':'2026-10-01','realtime_end':'2026-10-01','offset':0,'limit':50000,'complete_requested_window':True,'full_series_history':False,'point_in_time_backtest':False}

class Clocks(unittest.TestCase):
    def identity(self,m,acquired='2026-10-01T21:20:30Z',run='2026-10-01T21:21:00Z',cutoff='2026-09-30T21:37:13Z'):
        receipt,population=fixture();receipt['acquired_at']=acquired
        return m.api_identity('DGS10',receipt,population,m.clock(run),m.clock(cutoff))
    def test_predecessor_rejects_completed_acquisition_and_accepts_impossible_future_receipt(self):
        old=load(True);self.assertFalse(self.identity(old));self.assertTrue(self.identity(old,'2026-10-01T21:21:01Z'))
    def test_candidate_accepts_completed_acquisition_and_rejects_future_one(self):
        m=load();self.assertTrue(self.identity(m));self.assertFalse(self.identity(m,'2026-10-01T21:21:00.000001Z'))
    def test_inclusive_execution_bound_release_cutoff_and_zones(self):
        m=load()
        for acquired in ('2026-10-01T21:06:00Z','2026-10-01T21:21:00Z','2026-10-01T17:20:30-04:00'):self.assertTrue(self.identity(m,acquired))
        for acquired in ('2026-10-01T21:05:59.999999Z','2026-10-01T21:20:30','2026-09-30T21:37:12Z'):self.assertFalse(self.identity(m,acquired))
        self.assertFalse(self.identity(m,cutoff='2026-10-01T21:20:31Z'));self.assertTrue(self.identity(m,cutoff='2026-10-01T21:20:30Z'))
    def test_whole_qualification_keeps_all_existing_population_proof_requirements(self):
        scope=__import__('runpy').run_path(str(R/'tests/deployment/test_fifx_api_delivery_qualification.py'))
        archive=copy.deepcopy(scope['a']);before=archive['generated_at'];after=(load().clock(before)+timedelta(seconds=30)).isoformat()
        archive['generated_at']=after;archive['manifests'][0]['manifest']['generated_at']=after
        self.assertFalse(load(True).qualify(archive,scope['hashes'],scope['CUTOFF'])['api_originals_verified'])
        result=load().qualify(archive,scope['hashes'],scope['CUTOFF']);self.assertTrue(result['api_originals_verified']);self.assertEqual(len(result['fred_sources']),6)
        for key in ('investment_authority','current_pointer_delivery_verified','schedule_causation_verified','point_in_time_qualified'):self.assertFalse(result[key])
    def test_real_producer_stamps_inputs_after_serialized_acquisitions(self):
        import fifx_store as store
        clock=['2026-10-01T21:20:00Z'];captured={};calls=[];objects={}
        class Client:
            def put_object(self,**kw):
                self_key=kw['Key'];assert self_key.startswith(store.model.PRIVATE+'requests/');objects[self_key]=kw['Body'];return {}
            def get_object(self,**kw):
                raw=objects[kw['Key']];return {'Body':io.BytesIO(raw),'ContentLength':len(raw)}
        def read(key):
            assert key in (store.SOURCE,store.BOND);return b'{}'
        def collect(plan,begin,finish):
            calls.append('collect');begin('DGS10');clock[0]='2026-10-01T21:20:30Z'
            receipt,population=fixture();finish('DGS10',b'{}',receipt,{'status':'invented'});captured['receipt']=receipt;clock[0]='2026-10-01T21:21:00Z';calls.append('collect_completed')
        def retain(client,bucket,inputs):
            captured['inputs']=copy.deepcopy(inputs);calls.append('retain')
            return {'generated_at':inputs['generated_at'],'quality':{'status':'invented'},'replay':{}}
        with patch.object(store,'now',lambda:clock[0]),patch.object(store,'qualified_arithmetic'),patch.object(store,'previous_state',return_value=({},{})),patch.object(store,'reader',return_value=read),patch.object(store,'existing_move',return_value=(None,None)),patch.object(store,'retain_bytes',side_effect=lambda c,b,raw,*args,**kw:{'sha256':hashlib.sha256(raw).hexdigest()}),patch.object(store.acquisition,'plan',return_value={'DGS10':fixture()[0]['source_url']}),patch.object(store.acquisition,'collect',side_effect=collect),patch.object(store,'retain',side_effect=retain),patch.object(store,'publish',return_value=False):
            store.run(Client(),'invented-bucket','invented-request')
        self.assertEqual(calls,['collect','collect_completed','retain']);self.assertEqual(captured['inputs']['generated_at'],'2026-10-01T21:21:00Z')
        self.assertTrue(self.identity(load(),captured['receipt']['acquired_at'],captured['inputs']['generated_at']))
        self.assertFalse(self.identity(load(True),captured['receipt']['acquired_at'],captured['inputs']['generated_at']))
    def test_exact_predecessor_and_single_chronology_edit(self):
        plan=json.loads((D/'edits.json').read_bytes());raw=(D/'predecessor.py.txt').read_bytes();self.assertEqual(hashlib.sha256(raw).hexdigest(),plan['predecessor_sha256']);text=raw.decode()
        for a,b in plan['edits']:self.assertEqual(text.count(a),1);text=text.replace(a,b)
        self.assertEqual(text,(R/'aws/ops/checks/fifx_api_delivery.py').read_text(encoding='utf-8'));self.assertEqual(hashlib.sha256(text.encode()).hexdigest(),plan['candidate_sha256'])
if __name__=='__main__':unittest.main(verbosity=2)
