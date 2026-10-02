from pathlib import Path
from unittest.mock import patch
import gzip,hashlib,importlib.util,io,json,sys,types,unittest
R=Path(__file__).resolve().parents[1];D=R/'tests/fixtures/ticker-input-reader'
sys.path.insert(0,str(R/'aws/shared'))
def load(name,path):
 spec=importlib.util.spec_from_file_location(name,path);m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m);return m
hub=load('strict_ticker_hub',R/'aws/shared/ticker_360.py')
old=types.ModuleType('prior_hub');exec(compile((D/'shared-before.py.txt').read_bytes(),'<retained predecessor>','exec'),old.__dict__)
class Stream(io.BytesIO):
 def __init__(self,raw,fail=False):super().__init__(raw);self.sizes=[];self.fail=fail
 def read(self,size=-1):
  self.sizes.append(size)
  if self.fail:raise OSError('INVENTED_PRIVATE_CANARY')
  return super().read(size)
class Storage:
 def __init__(self,raw,**meta):self.raw=raw;self.meta=meta;self.bodies=[];self.reads=[];self.writes=[]
 def get_object(self,**kw):
  self.reads.append(kw);raw=self.raw[kw['Key']] if isinstance(self.raw,dict) else self.raw
  body=Stream(raw,self.meta.get('fail',False));self.bodies.append(body)
  return {'Body':body,'ContentLength':len(raw),**{k:v for k,v in self.meta.items() if k!='fail'}}
 def put_object(self,**kw):self.writes.append(kw);return {}
def raw(value):return json.dumps(value,ensure_ascii=False,allow_nan=False).encode()
def read(data,**meta):
 s=Storage(data,**meta);packet=hub._read_packet(s,'data/invented.json',{});return packet,s
class StrictTransport(unittest.TestCase):
 def test_valid_quantities_and_every_nested_field_survive(self):
  value={'by_ticker':{'QAONLY':{'zero':0,'ratio':.1,'tiny':1e-300,'large':1e200,'negative':-3.5,'flag':False,'missing':None,'nested':[{'label':'invented µ'}]}}}
  result,s=read(raw(value));self.assertEqual(result,value);self.assertTrue(s.bodies[0].closed);self.assertTrue(s.bodies[0].sizes);self.assertTrue(all(0<n<=65536 for n in s.bodies[0].sizes))
 def test_duplicate_members_do_not_select_last_value(self):
  for data in (b'{"score":1,"score":99}',b'{"by_ticker":{"QAONLY":{"score":1,"score":99}}}',b'{"x":1,"\\u0078":2}'):
   self.assertIsNotNone(old._read_packet(Storage(data),'data/invented.json',{}));result,s=read(data);self.assertIsNone(result);self.assertTrue(s.bodies[0].closed)
 def test_nonfinite_underflow_and_lost_decimal_precision_withhold(self):
  for token in (b'NaN',b'Infinity',b'-Infinity',b'1e309',b'1e-1000',b'0.123456789012345678901234567890'):
   data=b'{"quantity":'+token+b'}';self.assertIsNotNone(old._read_packet(Storage(data),'data/invented.json',{}));result,s=read(data);self.assertIsNone(result);self.assertTrue(s.bodies[0].closed)
 def test_underflow_predecessor_is_false_zero(self):
  data=b'{"quantity":1e-1000}';self.assertEqual(old._read_packet(Storage(data),'data/invented.json',{})['quantity'],0.0);self.assertIsNone(read(data)[0])
 def test_whole_content_length_is_required_and_typed(self):
  data=b'{"quantity":0}'
  for n in (None,True,str(len(data)),len(data)-1,len(data)+1,-1):
   result,s=read(data,ContentLength=n);self.assertIsNone(result);self.assertTrue(s.bodies[0].closed)
 def test_oversize_transport_is_bounded_closed_and_cached(self):
  data=b' '+raw({'quantity':1})+b' '*200;s=Storage(data);cache={}
  with patch.object(hub,'MAX_BYTES',64):
   self.assertIsNone(hub._read_packet(s,'data/invented.json',cache));self.assertIsNone(hub._read_packet(s,'data/invented.json',cache))
  self.assertEqual(len(s.reads),1);self.assertEqual(s.bodies[0].sizes,[]);self.assertTrue(s.bodies[0].closed)
 def test_lying_short_length_still_cannot_bypass_actual_byte_limit(self):
  s=Storage(b' '*200,ContentLength=1)
  with patch.object(hub,'MAX_BYTES',64):self.assertIsNone(hub._read_packet(s,'data/invented.json',{}))
  self.assertEqual(s.bodies[0].sizes,[65]);self.assertTrue(s.bodies[0].closed)
 def test_short_transport_chunks_are_consumed_to_eof(self):
  value={'by_ticker':{'QAONLY':{'value':0,'nested':[1,2,3]}}};s=Storage(raw(value));original=Stream.read
  with patch.object(Stream,'read',lambda self,n=-1:original(self,min(n,3))):packet=hub._read_packet(s,'data/invented.json',{})
  self.assertEqual(packet,value);self.assertGreater(len(s.bodies[0].sizes),5);self.assertTrue(s.bodies[0].closed)
 def test_gzip_valid_and_missing_header_are_decoded_without_quantity_change(self):
  value={'by_ticker':{'QAONLY':{'value':0}}};data=gzip.compress(raw(value),mtime=0)
  for encoding in ('gzip',' GZIP ',''):
   result,s=read(data,ContentEncoding=encoding);self.assertEqual(result,value);self.assertTrue(s.bodies[0].closed)
 def test_encoding_mismatch_truncated_and_multiple_gzip_members_rejected(self):
  plain=raw({'value':0});compressed=gzip.compress(plain,mtime=0)
  for data,encoding in ((plain,'gzip'),(compressed,'identity'),(compressed[:-1],'gzip'),(compressed+compressed,'gzip'),(plain,'br'),(plain,3),(plain,False),(plain,None)):
   self.assertIsNone(read(data,ContentEncoding=encoding)[0])
 def test_gzip_expansion_bound_rejects_small_bomb(self):
  data=gzip.compress(b' '*((16*1024*1024)+1),mtime=0);self.assertLess(len(data),100000);self.assertIsNone(read(data,ContentEncoding='gzip')[0])
 def test_read_failure_closes_stream_and_does_not_leak_body_or_error(self):
  s=Storage(b'{"secret":"INVENTED_PRIVATE_CANARY"}',fail=True)
  out=hub._domain_view(s,'fixture',{'key':'data/invented.json','kind':'packet','context':None},'QAONLY',{})
  self.assertFalse(out['available']);self.assertTrue(s.bodies[0].closed);self.assertNotIn('CANARY',json.dumps(out))
 def test_nonobjects_bad_utf8_and_truncated_json_are_unavailable(self):
  for data in (b'null',b'[]',b'false',b'1',b'"text"',b'{"value":1',b'{"x":"\xff"}'):
   result,s=read(data);self.assertIsNone(result);self.assertTrue(s.bodies[0].closed)
 def test_shared_cache_does_not_read_same_good_or_bad_source_again(self):
  s=Storage({'data/good.json':raw({'by_ticker':{'QAONLY':{'value':0}}}),'data/bad.json':b'{"x":NaN}'});cache={}
  for _ in range(3):
   self.assertEqual(hub._read_packet(s,'data/good.json',cache)['by_ticker']['QAONLY']['value'],0);self.assertIsNone(hub._read_packet(s,'data/bad.json',cache))
  self.assertEqual(len(s.reads),2);self.assertTrue(all(body.closed for body in s.bodies))
 def test_actual_producer_keeps_good_domain_when_another_has_nonfinite_input(self):
  value={'generated_at':'2026-10-02T00:00:00Z','by_ticker':{'QAONLY':{'value':0}}}
  s=Storage({'data/good.json':raw(value),'data/bad.json':b'{"by_ticker":{"QAONLY":{"value":NaN}}}'})
  sources={k:{'key':'data/'+k+'.json','kind':'packet','context':None} for k in ('good','bad')};boto=types.ModuleType('boto3');boto.client=lambda *a,**k:s
  with patch.dict(sys.modules,{'boto3':boto,'ticker_360':hub}),patch.object(hub,'SOURCES',sources):
   producer=load('strict_reader_producer',R/'aws/lambdas/justhodl-ticker-360/source/lambda_function.py');result=producer.lambda_handler({},None)
  self.assertTrue(result['ok']);self.assertEqual(len(s.writes),1);out=json.loads(s.writes[0]['Body']);self.assertEqual(out['tickers']['QAONLY']['domains']['good']['data'],{'value':0});self.assertEqual(set(out['tickers']['QAONLY']['domains']),{'good'});self.assertFalse(out['calls_eligible']);self.assertEqual(len(s.reads),2)
 def test_invalid_primary_and_valid_fallback_preserve_actual_origin_and_cache(self):
  value={'by_ticker':{'QAONLY':{'value':0}}};s=Storage({'data/primary.json':b'{"value":NaN}','data/fallback.json':raw(value)})
  spec={'key':'data/primary.json','fallback_key':'data/fallback.json','context':None,'kind':'packet'};cache={}
  for _ in range(2):
   out=hub._domain_view(s,'fixture',spec,'QAONLY',cache);self.assertEqual(out['source_key'],'data/fallback.json');self.assertEqual(out['configured_source_key'],'data/primary.json');self.assertTrue(out['fallback_source_used']);self.assertEqual(out['ticker_data'],{'value':0});self.assertFalse(out['calls_eligible'])
  self.assertIsNone(cache['data/primary.json']);self.assertEqual(cache['data/fallback.json'],value);self.assertEqual(len(s.reads),2);self.assertTrue(all(b.closed for b in s.bodies))
 def test_invalid_fallback_stays_unavailable_without_reopening_streams(self):
  s=Storage({'data/primary.json':b'{"value":NaN}','data/fallback.json':b'{"value":1e-1000}'});cache={}
  for _ in range(2):self.assertIsNone(hub._read_packet(s,'data/primary.json',cache,'data/fallback.json'))
  self.assertEqual(len(s.reads),2);self.assertTrue(all(b.closed for b in s.bodies))
 def test_only_reviewed_input_boundary_bytes_change(self):
  t=json.loads((D/'transition.json').read_bytes());text=(R/'aws/shared/ticker_360.py').read_text(encoding='utf-8');self.assertEqual(hashlib.sha256(text.encode()).hexdigest(),t['after_sha256'])
  for change in reversed(t['replacements']):self.assertEqual(text.count(change['after']),1);text=text.replace(change['after'],change['before'])
  self.assertEqual(text,(D/'shared-before.py.txt').read_text(encoding='utf-8'));self.assertEqual(hashlib.sha256(text.encode()).hexdigest(),t['before_sha256'])

class PriorAssertions(unittest.TestCase):
 def test_all_prior_guard_assertions_remain_with_only_reader_compatibility_hooks(self):
  t=json.loads((D/'guard-test-transition.json').read_bytes());text=(D/'guard-test-before.py.txt').read_text(encoding='utf-8')
  self.assertEqual(hashlib.sha256(text.encode()).hexdigest(),t['before_sha256'])
  for e in t['replacements']:self.assertEqual(text.count(e['before']),1);text=text.replace(e['before'],e['after'])
  self.assertEqual(text,(R/t['path']).read_text(encoding='utf-8'));self.assertEqual(hashlib.sha256(text.encode()).hexdigest(),t['after_sha256'])

if __name__=='__main__':unittest.main(verbosity=2)
