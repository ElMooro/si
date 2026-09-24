from pathlib import Path
from unittest.mock import Mock
import copy,sys,unittest
ROOT=Path(__file__).resolve().parents[1]
sys.path[:0]=[str(ROOT/'aws/ops/staged'),str(ROOT/'tests')]
import ops_6028_flow_state_source_preflight as op
from test_option_flow_store import S3

def fixture():
    key='data/etf-true-flows.json';contract,prefix,run_contract=op.PARENTS[key];prefix='data/'+prefix+'/'
    blobs={}
    def ref(value,kind,ext='json'):
        body=value if isinstance(value,bytes) else op.encoded(value);sha=op.sha(body);path=prefix+kind+'/'+sha+'.'+ext
        blobs[path]=body;return {'key':path,'sha256':sha,'bytes':len(body)}
    output={'contract':contract,'generated_at':'2026-09-24T15:00:00Z',
        'calls_eligible':False,'sizing_eligible':False,'execution_eligible':False,'measurements':[None,0]}
    out=ref(output,'outputs');run={'contract':run_contract,'generated_at':output['generated_at'],
        'input':ref({'sources':[]},'inputs'),'output':out,'output_sha256':out['sha256'],
        'compilers':{'model':ref(b'# preserved compiler\n','compilers','py')}}
    identity={'manifest_key':ref(run,'runs')['key'],'output_sha256':out['sha256']}
    return key,{**output,'replay':identity},blobs,run

class Tests(unittest.TestCase):
    def test_complete_bytes_retained_without_normalizing_unknown_fields(self):
        key=op.PACKETS[0];raw=b'{ "unknown":[0,null],"headline":"older claim" }\n';s=S3({key:raw})
        capture,doc=op.capture(s,key)
        self.assertEqual(op.original(s,capture['original']),raw)
        self.assertEqual(doc['unknown'],[0,None])
        self.assertTrue(all(x['Key'].startswith(op.PRIVATE) for x in s.writes))

    def test_unreviewed_packet_cannot_be_read(self):
        s=Mock()
        for key in ('data/trade-tickets.json','portfolio/current.json','https://example.com/x'):
            with self.assertRaises(AssertionError):op.capture(s,key)
        s.get_object.assert_not_called()

    def test_exact_parent_hash_binding_does_not_claim_original_replay_or_forecast(self):
        key,packet,blobs,_=fixture();result=op.bind_parent(key,packet,blobs.__getitem__)
        self.assertTrue(result['run_input_output_compiler_hashes_match'])
        self.assertFalse(result['source_original_replay_performed_here'])
        self.assertFalse(result['forecast_qualification_performed'])

    def test_tampered_packet_and_artifact_are_rejected(self):
        key,packet,blobs,run=fixture();bad=copy.deepcopy(packet);bad['measurements']=[0,0]
        with self.assertRaises(AssertionError):op.bind_parent(key,bad,blobs.__getitem__)
        blobs[run['output']['key']]+=b' '
        with self.assertRaises(AssertionError):op.bind_parent(key,packet,blobs.__getitem__)

    def test_unreviewed_run_path_and_new_authority_are_rejected_before_read(self):
        key,packet,_,_=fixture();read=Mock()
        for change in (lambda p:p['replay'].update(manifest_key='data/trade-tickets.json'),lambda p:p.update(sizing_eligible=True)):
            bad=copy.deepcopy(packet);change(bad)
            with self.assertRaises(AssertionError):op.bind_parent(key,bad,read)
        read.assert_not_called()

    def test_foreign_compiler_path_rejected_before_fetch(self):
        key,packet,blobs,run=fixture();run['compilers']['model']['key']='data/trade-tickets.json'
        body=op.encoded(run);path='data/etf-research/runs/'+op.sha(body)+'.json';blobs[path]=body
        packet['replay']['manifest_key']=path;requested=[]
        def read(k):requested.append(k);return blobs[k]
        with self.assertRaises(AssertionError):op.bind_parent(key,packet,read)
        self.assertNotIn('data/trade-tickets.json',requested)

    def test_inventory_does_not_turn_generation_time_into_observation_time(self):
        result=op.describe({'generated_at':'2026-09-24T00:00:00Z'})
        self.assertIsNone(result['as_of']);self.assertIsNone(result['data_asof'])
        self.assertFalse(result['source_original_replay_performed_here'])
        self.assertIsNone(result['reported_authority']['calls_eligible'])

if __name__=='__main__':unittest.main(verbosity=2)
