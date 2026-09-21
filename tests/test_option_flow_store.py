from pathlib import Path
from unittest.mock import patch
from threading import Lock
import copy,hashlib,io,json,sys,unittest
ROOT=Path(__file__).resolve().parents[1];sys.path[:0]=[str(ROOT/'aws/shared'),str(ROOT/'tests')]
import option_flow_store as store
import option_flow_research as model
from test_option_flow_research import fixture


class Error(Exception):
    def __init__(self,code):self.response={'Error':{'Code':code}}


class S3:
    def __init__(self,data=None):self.data=dict(data or {});self.writes=[];self.lock=Lock()
    def get_object(self,**kw):
        key=kw['Key']
        with self.lock:
            if key not in self.data:raise Error('NoSuchKey')
            raw=self.data[key];return {'Body':io.BytesIO(raw),'ETag':hashlib.sha256(raw).hexdigest()}
    def put_object(self,**kw):
        with self.lock:
            key=kw['Key'];old=self.data.get(key)
            if kw.get('IfNoneMatch')=='*' and old is not None:raise Error('PreconditionFailed')
            if 'IfMatch' in kw and (old is None or kw['IfMatch']!=hashlib.sha256(old).hexdigest()):raise Error('PreconditionFailed')
            self.data[key]=kw['Body'];self.writes.append(kw)


class StoreTests(unittest.TestCase):
    def setUp(self):self.patch=patch.object(model,'CONTINUITY',('SPY',));self.patch.start()
    def tearDown(self):self.patch.stop()
    def candidate(self):
        blobs,inputs=fixture();s3=S3(blobs);read=store.reader(s3,'bucket')
        with store.ArtifactWriter(s3,'bucket',read) as emit:out=store.compile_output(inputs,read,emit)
        ref=store.retain(s3,'bucket',inputs,out,read)
        return s3,inputs,out,ref
    def test_replay_has_no_provider_or_public_head_side_effect(self):
        s3,inputs,out,ref=self.candidate();before=dict(s3.data)
        self.assertEqual(store.replay(ref,store.reader(s3,'bucket')),out);self.assertEqual(before,s3.data)
        self.assertNotIn(model.CURRENT,s3.data);self.assertNotIn(model.LEGACY,s3.data)
    def test_replay_rejects_compiler_tamper(self):
        s3,inputs,out,ref=self.candidate();run=json.loads(s3.data[ref['manifest_key']]);key=next(iter(run['compilers'].values()))['key'];s3.data[key]+=b'\n'
        with self.assertRaises(ValueError):store.replay(ref,store.reader(s3,'bucket'))
    def test_replay_rejects_record_tamper(self):
        s3,inputs,out,ref=self.candidate();key=next(k for k in s3.data if '/rows/' in k);s3.data[key]+=b' '
        with self.assertRaises(ValueError):store.replay(ref,store.reader(s3,'bucket'))
    def test_arbitrary_private_read_refused(self):
        s3=S3()
        with self.assertRaises(ValueError):store.reader(s3,'bucket')('data/trade-tickets.json')
        with self.assertRaises(ValueError):store.snapshot(s3,'bucket','data/trade-tickets.json',store.reader(s3,'bucket'))
    def test_immutable_conflict_cannot_replace_existing_bytes(self):
        raw=b'{}';key=model.ref(raw,'inputs')['key'];s3=S3({key:b'bad'})
        with self.assertRaises(ValueError):store.immutable(s3,'bucket',key,raw)
        self.assertEqual(s3.data[key],b'bad')
    def test_conditional_cannot_roll_back_or_overwrite_same_clock(self):
        new={'generated_at':'2026-09-21T12:00:00Z'};s3=S3({model.CURRENT:model.encoded(new)})
        self.assertFalse(store.conditional(s3,'bucket',model.CURRENT,{'generated_at':'2026-09-20T12:00:00Z'}))
        with self.assertRaises(ValueError):store.conditional(s3,'bucket',model.CURRENT,{**new,'changed':True})
    def test_whole_predecessor_preserved_before_write(self):
        old=model.encoded({'generated_at':'2026-09-20T12:00:00Z','all_results':[{'every':'field'}]});s3=S3({model.CURRENT:old})
        self.assertTrue(store.conditional(s3,'bucket',model.CURRENT,{'generated_at':'2026-09-21T12:00:00Z'}))
        self.assertEqual(s3.data[model.PRIVATE+model.sha(old)+'.bin'],old)
        self.assertIn('IfMatch',s3.writes[-1])
    def test_retirement_of_later_legacy_scan_preserves_its_original_clock(self):
        old=model.encoded({'engine':'justhodl-polygon-options-flow','version':'2.0.0','generated_at':'2026-09-21T13:00:00Z',
            'all_results':[{'old':'directional-alert'}],'universe':['SPY']});s3=S3({model.LEGACY:old})
        retirement={'contract':'option-flow-compatibility.v1','generated_at':'2026-09-21T12:00:00Z','all_results':[]}
        self.assertTrue(store.conditional(s3,'bucket',model.LEGACY,retirement))
        self.assertEqual(s3.data[model.PRIVATE+model.sha(old)+'.bin'],old)
        self.assertEqual(json.loads(s3.data[model.LEGACY])['generated_at'],'2026-09-21T12:00:00Z')
    def test_duplicate_request_does_not_recollect_or_republish(self):
        key=store.request_key('test');result={'status':'complete','request_id':'test'};s3=S3({key:model.encoded(result)})
        with patch.object(store,'collect',side_effect=AssertionError('Must not recollect')):
            self.assertEqual(store.run(s3,'bucket','test','execution'),result)
        self.assertEqual(s3.writes,[])
    def test_recovery_keeps_original_clocks_and_no_collection(self):
        s3,inputs,out,ref=self.candidate()
        with patch.object(store,'collect',side_effect=AssertionError('Must not recollect')):
            result=store.run(s3,'bucket','recovery','execution',recover_run=ref)
        self.assertTrue(result['published']);self.assertTrue(result['compatibility_published'])
        self.assertEqual(result['provider_requests_this_execution'],0)
        self.assertEqual(json.loads(s3.data[model.CURRENT])['generated_at'],inputs['generated_at'])
        alias=json.loads(s3.data[model.LEGACY]);self.assertEqual(alias['all_results'],[]);self.assertFalse(alias['calls_eligible'])
    def test_failed_replay_records_failure_and_keeps_public_packet(self):
        s3,inputs,out,ref=self.candidate();current=b'{"old":"whole"}';s3.data[model.CURRENT]=current
        first=inputs['chains']['SPY']['pages'][0]['original']['key'];s3.data[first]=b'bad'
        with self.assertRaises(RuntimeError):store.run(s3,'bucket','fail','execution',recover_run=ref)
        self.assertEqual(s3.data[model.CURRENT],current)
        self.assertEqual(json.loads(s3.data[store.request_key('fail')])['status'],'failed')
    def test_writer_retention_failure_is_fatal_not_partial_source(self):
        s3=S3();key=store.request_key('collect')
        def fake(symbol,secret,deadline,retain,budget,checkpoint):retain(b'{}')
        with patch.object(store.capture,'collect',side_effect=fake),patch.object(store,'protect',side_effect=ValueError('readback differs')):
            with self.assertRaises(RuntimeError):store.collect(s3,'bucket','configured',{'selected':['SPY']},store.reader(s3,'bucket'),1,key)
    def test_request_path_cannot_escape_private_namespace(self):
        for identity in ('../public', 'x'*161, ''):
            with self.assertRaises(ValueError):store.request_key(identity)
        with self.assertRaises(ValueError):store.status_write(S3(),'bucket',model.CURRENT,{})


if __name__=='__main__':unittest.main()
