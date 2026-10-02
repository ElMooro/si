from pathlib import Path
from datetime import datetime,timezone
from unittest.mock import Mock,patch
import copy,importlib.util,io,json,sys,types,unittest
W=Path(__file__).resolve().parents[1]/'aws/shared'
def module(name,path):
 spec=importlib.util.spec_from_file_location(name,path);m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m);return m
M=module('crypto_sentiment_observations',W/'crypto_sentiment_observations.py')
T=module('crypto_sentiment_transport',W/'crypto_sentiment_transport.py')
with patch.dict(sys.modules,{'crypto_sentiment_observations':M,'crypto_sentiment_transport':T}):
 A=module('crypto_sentiment_archive',W/'crypto_sentiment_archive.py')
class Exists(Exception):
 def __init__(self):self.response={'Error':{'Code':'412'}}
class Store:
 def __init__(self):self.data={};self.calls=[];self.closed=False
 def put_object(self,**kw):
  self.calls.append(('put',kw['Key']));assert kw['IfNoneMatch']=='*'
  if kw['Key'] in self.data:raise Exists()
  self.data[kw['Key']]=kw['Body']
 def get_object(self,**kw):
  self.calls.append(('get',kw['Key']));raw=self.data[kw['Key']]
  class Short(io.BytesIO):
   def read(self,n):return super().read(min(3,n))
  return {'Body':Short(raw),'ContentLength':len(raw)}
 def close(self):self.closed=True
class Response(io.BytesIO):
 status=200;headers={}
 def __init__(self,raw,url):super().__init__(raw);self.url=url
 def geturl(self):return self.url
RAW=b'{"name":"Fear and Greed Index","data":[{"value":"0","timestamp":"1577836800"}],"metadata":{"error":null}}'
def opener(request,timeout):return Response(RAW,request.full_url)
def packet(raw=None):
 return T.collect(lambda request,timeout:Response(raw or RAW,request.full_url),M,lambda:180000,now=lambda:datetime(2020,1,1,tzinfo=timezone.utc))
class Archive(unittest.TestCase):
 def test_whole_original_replay_and_idempotent_conditional_retention(self):
  p=packet();client=Store();ref=A.retain(client,'invented',p);self.assertEqual(A.replay(client,'invented',ref['manifest']),p)
  before=copy.deepcopy(client.data);self.assertEqual(A.retain(client,'invented',p),ref);self.assertEqual(client.data,before);self.assertEqual(len(client.data),5)
  self.assertTrue(all(key.startswith(A.PREFIX) for key in client.data));self.assertFalse(ref['investment_authority'])
 def test_replay_reads_only_originals_and_never_writes(self):
  client=Store();ref=A.retain(client,'invented',packet());client.calls=[];A.replay(client,'invented',ref['manifest']);self.assertTrue(all(kind=='get' for kind,_ in client.calls))
 def test_missing_and_invalid_response_can_be_replayed_without_recovery_claim(self):
  for p in [packet(b'{}'),T.collect(Mock(side_effect=TimeoutError()),M,lambda:180000,now=lambda:datetime(2020,1,1,tzinfo=timezone.utc))]:
   client=Store();ref=A.retain(client,'invented',p);out=A.replay(client,'invented',ref['manifest']);self.assertIsNone(out['current']);self.assertFalse(out['calls_eligible'])
 def test_changed_computation_or_permission_fails_before_storage(self):
  for change in [lambda p:p.update(current=99),lambda p:p.update(calls_eligible=True),lambda p:p['full_history'][0].update(value=9),lambda p:p['source_attempts'][0].update(received_bytes=0)]:
   p=packet();change(p);client=Store()
   with self.assertRaises(ValueError):A.retain(client,'invented',p)
   self.assertEqual(client.calls,[])
 def test_corrupted_readback_is_not_publishable(self):
  client=Store();original=client.get_object
  def bad(**kw):
   value=original(**kw);value['Body']=io.BytesIO(b'corrupt');return value
  client.get_object=bad
  with self.assertRaises(ValueError):A.retain(client,'invented',packet())
 def test_foreign_current_or_private_namespace_is_rejected_before_read(self):
  client=Store();ref=A.reference('captures',b'{}')
  for key in ['crypto-intel.json','private/account.json','data/crypto-funding-research/captures/'+ref['sha256']+'.json']:
   with self.assertRaises(ValueError):A.complete_read(client,'invented',{**ref,'key':key},'captures')
  self.assertEqual(client.calls,[])
 def test_compiler_original_is_never_executed_or_accepted_if_foreign(self):
  client=Store();ref=A.retain(client,'invented',packet());manifest=A.strict(client.data[ref['manifest']['key']])
  foreign=b'raise RuntimeError("must never execute")';manifest['compilers']['crypto_sentiment_transport.py']=A.retain_bytes(client,'invented','compilers',foreign)
  modified=A.retain_bytes(client,'invented','runs',A.encode(manifest))
  with self.assertRaisesRegex(ValueError,'Compiler differs'):A.replay(client,'invented',modified)
 def test_manifest_clock_permission_and_compiler_closure_cannot_change(self):
  for change in [lambda m:m.update(calls_eligible=True),lambda m:m.update(acquisition_completed_at='2021-01-01T00:00:00Z'),lambda m:m['compilers'].pop('crypto_sentiment_transport.py')]:
   client=Store();ref=A.retain(client,'invented',packet());manifest=A.strict(client.data[ref['manifest']['key']]);change(manifest);modified=A.retain_bytes(client,'invented','runs',A.encode(manifest))
   with self.assertRaises(ValueError):A.replay(client,'invented',modified)
 def test_uncertain_storage_write_is_not_retried(self):
  client=Store();client.put_object=Mock(side_effect=TimeoutError())
  with self.assertRaises(TimeoutError):A.retain(client,'invented',packet())
  self.assertEqual(client.put_object.call_count,1)
 def test_budget_denial_prevents_storage_and_masks_backend_details(self):
  factory=Mock();open_mock=Mock(side_effect=opener)
  with self.assertRaisesRegex(ValueError,'originals unavailable'):
   A.collect_retained(open_mock,factory,'invented',lambda:29999)
  factory.assert_not_called();self.assertEqual(open_mock.call_count,0)
 def test_success_closes_client_and_publishes_original_reference(self):
  client=Store();out=A.collect_retained(opener,lambda:client,'invented',lambda:60000)
  self.assertTrue(client.closed);self.assertTrue(out['original_capture']['complete_capture_replayed']);self.assertFalse(out['calls_eligible'])
 def test_failure_closes_client_and_never_leaks_error_text(self):
  client=Store();client.put_object=Mock(side_effect=PermissionError('https://private.invalid?signature=secret'))
  with self.assertRaises(ValueError) as ctx:A.collect_retained(opener,lambda:client,'invented',lambda:60000)
  self.assertTrue(client.closed);self.assertNotIn('signature',str(ctx.exception))
 def test_consumer_replays_original_and_binds_capture_without_storage_or_vote(self):
  client=Store();p=packet();proof=A.retain(client,'invented',p);client.calls=[]
  with patch.dict(sys.modules,{'crypto_sentiment_transport':T}):out=M.context({**p,'original_capture':proof})
  self.assertEqual(out['status'],'descriptive');self.assertEqual(out['current'],0);self.assertTrue(out['original_projection_checked']);self.assertFalse(out['stored_archive_read_verified']);self.assertEqual(client.calls,[])
  self.assertTrue(all(out[k]==v for k,v in M.DENIED.items()))
 def test_consumer_rejects_legacy_corrupt_or_unretained_computation(self):
  client=Store();p=packet();published={**p,'original_capture':A.retain(client,'invented',p)}
  mutations=[lambda d:d.update(current=9),lambda d:d.update(calls_eligible=True),lambda d:d.pop('original_capture'),lambda d:d['original_capture']['capture'].update(sha256='a'*64,key=A.PREFIX+'captures/'+'a'*64+'.json'),lambda d:d['source_attempts'][0].update(http_status=503)]
  with patch.dict(sys.modules,{'crypto_sentiment_transport':T}):
   for mutation in mutations:
    changed=copy.deepcopy(published);mutation(changed);self.assertEqual(M.context(changed)['status'],'unavailable')
   self.assertEqual(M.context({'current':50})['status'],'unavailable')
if __name__=='__main__':unittest.main(verbosity=2)
