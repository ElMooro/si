from unittest.mock import patch
import copy,io,json,unittest
from volatility_fixtures import model,fixture,originals,STAMP,EVALUATION
import volatility_research_store as store

class Conflict(Exception):
    def __init__(self,code):self.response={'Error':{'Code':code}}


class Memory:
    def __init__(self):self.objects={};self.reads=[];self.writes=[]
    def get_object(self,Bucket,Key):
        self.reads.append(Key)
        if Key not in self.objects:raise Conflict('NoSuchKey')
        raw=self.objects[Key];return {'Body':io.BytesIO(raw),'ETag':model.sha(raw)}
    def put_object(self,Bucket,Key,Body,**kw):
        current=self.objects.get(Key)
        if kw.get('IfNoneMatch')=='*' and current is not None:raise Conflict('PreconditionFailed')
        if kw.get('IfMatch') is not None and (current is None or model.sha(current)!=kw['IfMatch']):raise Conflict('PreconditionFailed')
        self.objects[Key]=Body;self.writes.append((Key,kw));return {}


class StorageCases(unittest.TestCase):
    def test_original_replay_and_output_integrity(self):
        inputs,bodies=fixture();s=Memory();s.objects.update(bodies)
        out=store.compile_output(inputs,store.reader(s,'test'));ref=store.retain(s,'test',inputs,out)
        self.assertEqual(store.replay(ref,store.reader(s,'test')),out)
        key=next(iter(bodies));s.objects[key]=b'{}'
        with self.assertRaises(ValueError):store.replay(ref,store.reader(s,'test'))

    def test_mutated_compiler_cannot_replay(self):
        inputs,bodies=fixture();s=Memory();s.objects.update(bodies)
        out=store.compile_output(inputs,store.reader(s,'test'));ref=store.retain(s,'test',inputs,out)
        manifest=json.loads(s.objects[ref['manifest_key']]);key=next(iter(manifest['compilers'].values()))['key'];s.objects[key]=b'changed'
        with self.assertRaises(ValueError):store.replay(ref,store.reader(s,'test'))

    def test_whole_predecessor_is_preserved_before_cas(self):
        s=Memory();old=model.encoded({'generated_at':'2026-09-18T00:00:00Z','data_date':'2026-09-17','unrelated':{'preserve':'whole'}})
        s.objects[store.CURRENT]=old
        packet={'generated_at':STAMP,'as_of':'2026-09-17','sample':None}
        self.assertTrue(store.publish(s,'test',packet));self.assertEqual(s.objects[store.PRIVATE+model.sha(old)+'.bin'],old)
        self.assertEqual(json.loads(s.objects[store.CURRENT]),packet)

    def test_newer_or_later_observation_cannot_be_replaced(self):
        for old in ({'generated_at':'2026-09-21T00:00:00Z','as_of':'2026-09-17'},
                    {'generated_at':'2026-09-19T00:00:00Z','as_of':'2026-09-18'}):
            s=Memory();s.objects[store.CURRENT]=model.encoded(old)
            self.assertFalse(store.publish(s,'test',{'generated_at':STAMP,'as_of':'2026-09-17'}));self.assertEqual(s.writes,[])

    def test_same_clock_conflict_is_rejected(self):
        s=Memory();s.objects[store.CURRENT]=model.encoded({'generated_at':STAMP,'as_of':'2026-09-17','value':1})
        with self.assertRaises(ValueError):store.publish(s,'test',{'generated_at':STAMP,'as_of':'2026-09-17','value':2})

    def test_idempotent_retry_never_reacquires(self):
        s=Memory();key=store.request_key('test-request');prior={'status':'running'};s.objects[key]=model.encoded(prior)
        with patch.object(store,'collect',side_effect=AssertionError('must not reacquire')):
            self.assertEqual(store.run(s,'test','test-request','execution','test-key'),prior)

    def test_private_paths_and_content_addresses_are_enforced(self):
        s=Memory()
        for key in ('brain.json','data/accounts/x','../secret',store.PRIVATE+'x.bin'):
            with self.assertRaises(ValueError):store.reader(s,'test')(key)
        with self.assertRaises(ValueError):store.immutable(s,'test',store.PRIVATE+'0'*64+'.bin',b'{}')

    def test_source_credential_never_retained_or_redirected(self):
        class Response(io.BytesIO):status=200
        class Open:
            def open(self,request,timeout):return Response(b'{"reflected":"test-credential"}')
        s=Memory();c=store.Collector(s,'test','test-credential',EVALUATION,store.time.monotonic()+15,Open())
        self.assertEqual(c.acquire('VIX_30D','observations'),{'error':'provider_body_rejected'});self.assertEqual(s.writes,[])
        with self.assertRaises(ValueError):store.NoRedirect().redirect_request(None,None,302,'',{},'https://other.invalid')

    def test_live_run_uses_only_reviewed_original_paths(self):
        inputs,bodies=fixture();s=Memory();s.objects.update(bodies)
        clocks=iter([inputs['started_at'],inputs['generated_at'],inputs['generated_at']])
        with patch.object(store,'collect',return_value=inputs['sources']),patch.object(store,'now',side_effect=lambda:next(clocks)):
            result=store.run(s,'test','fresh-request','execution','synthetic-credential')
        self.assertEqual(result['status'],'complete');self.assertTrue(result['published'])
        self.assertTrue(all(k.startswith((store.PREFIX,store.PRIVATE)) or k==store.CURRENT for k in s.reads))
        self.assertEqual(result['notifications_sent'],0);self.assertEqual(result['private_account_reads'],0)


class PublisherCases(unittest.TestCase):
    def test_fred_credential_is_never_sent_to_publisher(self):
        requests=[]
        class Response(io.BytesIO):status=200
        class Open:
            def open(self,request,timeout):requests.append(request.full_url);return Response(originals('VVIX')['observations'])
        s=Memory();c=store.Collector(s,'test','synthetic-fred-key',EVALUATION,store.time.monotonic()+15,Open())
        result=c.acquire('VVIX','observations')
        self.assertEqual(requests,[model.source_url('VVIX','observations',EVALUATION)])
        self.assertEqual(result['evidence']['provider'],'Cboe');self.assertNotIn('api_key',result['evidence']['request_url'])
        self.assertEqual(len(s.writes),1)

    def test_missing_fred_key_does_not_block_public_publisher(self):
        class Response(io.BytesIO):status=200
        class Open:
            def open(self,request,timeout):return Response(originals('SKEW')['observations'])
        c=store.Collector(Memory(),'test',None,EVALUATION,store.time.monotonic()+15,Open())
        self.assertEqual(c.acquire('VIX_30D','observations'),{'error':'configured_credential_unavailable'})
        self.assertEqual(c.acquire('SKEW','observations')['evidence']['provider'],'Cboe')

    def test_changed_source_identity_clock_or_pair_inventory_rejected(self):
        inputs,bodies=fixture()
        for mutate in ('identity','clock','inventory'):
            x=copy.deepcopy(inputs)
            if mutate=='identity':x['sources']['VVIX']['observations']['evidence']['request_url']=model.source_url('SKEW','observations',EVALUATION)
            elif mutate=='clock':x['sources']['VVIX']['observations']['acquired_at']='2026-09-21T00:00:00+00:00'
            else:x['sources']['VVIX']['definition']=x['sources']['VVIX']['observations']
            with self.assertRaises(ValueError):store.compile_output(x,bodies.__getitem__)


if __name__=='__main__':unittest.main(verbosity=2)
