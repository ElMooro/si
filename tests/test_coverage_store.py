"""Complete invented storage/retention/CAS cases. Never uses real AWS."""
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from unittest.mock import patch
import hashlib,io,json,sys,unittest
HERE=Path(__file__).resolve().parent
SOURCE=HERE if (HERE/'coverage_store.py').exists() else HERE.parent/'aws/lambdas/justhodl-coverage-gap-report/source'
sys.path.insert(0,str(SOURCE))
import coverage_store as store
import coverage_model as model

class Error(Exception):
 def __init__(self,code):self.response={'Error':{'Code':code}}

class S3:
 def __init__(self):
  self.data={};self.reads=[];self.puts=[];self.streams=[];self.errors={};self.lengths={};self.encodings={};self.conflict=False;self.ack_lost=False
 def get_object(self,**kw):
  key=kw['Key'];self.reads.append(key)
  if key in self.errors:raise self.errors[key]
  if key not in self.data:raise Error('NoSuchKey')
  raw=self.data[key];body=io.BytesIO(raw);self.streams.append(body)
  return {'Body':body,'ContentLength':self.lengths.get(key,len(raw)),'ETag':'"observed-prior"','ContentEncoding':self.encodings.get(key,'')}
 def put_object(self,**kw):
  key=kw['Key'];self.puts.append(kw)
  if key==store.HEAD and self.conflict:raise Error('PreconditionFailed')
  if kw.get('IfNoneMatch')=='*' and key in self.data:raise Error('PreconditionFailed')
  self.data[key]=kw['Body']
  if key==store.HEAD and self.ack_lost:raise TimeoutError('invented lost acknowledgement')

class Tests(unittest.TestCase):
 def setUp(self):
  self.tmp=TemporaryDirectory();self.addCleanup(self.tmp.cleanup)
  paths={n:Path(self.tmp.name)/n for n in ('lambda_function.py','coverage_model.py','coverage_store.py')}
  for n,p in paths.items():p.write_bytes(('# complete invented compiler fixture: '+n+'\n').encode())
  self.client=S3();self.compilers=paths
  self.publish=lambda context:store.publish(self.client,'invented-bucket',self.compilers,context)
  self.context=SimpleNamespace(get_remaining_time_in_millis=lambda:120000)
  self.client.data[model.INPUTS['symbology']]=store.encoded({'n_tickers':99,'by_ticker':{'TEST':{'figi':'BBG000000001','cusip':'123456789'}}})
  self.client.data[model.INPUTS['edgar']]=store.encoded({'n_filings':2})
  self.client.data[model.INPUTS['nyfed']]=store.encoded({'rates':{'SOFR':{'n_obs':3}}})
  self.client.data[model.INPUTS['rollup']]=store.encoded({'global_feed_counts':{'fred':4}})
 def public_puts(self):return [kw for kw in self.client.puts if kw['Key']==store.HEAD]
 def test_true_absence_creates_conditionally_and_retains_exact_inputs(self):
  original=dict(self.client.data);packet,_=self.publish(self.context)
  self.assertEqual(self.public_puts()[0]['IfNoneMatch'],'*')
  for name,item in packet['sources'].items():
   ref=item['original_ref'];raw=original[model.INPUTS[name]]
   self.assertEqual(self.client.data[ref['key']],raw);self.assertEqual(ref['sha256'],hashlib.sha256(raw).hexdigest())
  self.assertTrue(all(body.closed for body in self.client.streams))
 def test_prior_read_failure_or_invalid_prior_prevents_any_replacement(self):
  for failure in (Error('AccessDenied'),TimeoutError()):
   self.client.errors[store.HEAD]=failure
   with self.assertRaises(Exception):self.publish(self.context)
   self.assertFalse(self.client.puts)
  self.client.errors.clear()
  for raw in (b'null',b'[]',b'{"x":1,"x":2}',b'{'):
   self.client.data[store.HEAD]=raw
   with self.assertRaises(Exception):self.publish(self.context)
   self.assertFalse(self.client.puts)
 def test_unknown_prior_fields_and_whole_predecessor_are_preserved(self):
  raw=store.encoded({'extra':{'complete':[1,None]},'as_of':'old'});self.client.data[store.HEAD]=raw
  packet,_=self.publish(self.context)
  self.assertEqual(packet['extra'],{'complete':[1,None]});self.assertEqual(self.public_puts()[0]['IfMatch'],'"observed-prior"')
  self.assertEqual(self.client.data[packet['replay']['previous_publication']['key']],raw)
 def test_conflicting_writer_never_gets_an_unconditional_retry(self):
  old=store.encoded({'original':True});self.client.data[store.HEAD]=old;self.client.conflict=True
  with self.assertRaises(store.PublicationUncertain):self.publish(self.context)
  self.assertEqual(self.client.data[store.HEAD],old);self.assertEqual(len(self.public_puts()),1)
 def test_lost_acknowledgement_is_not_claimed_as_rollback(self):
  self.client.data[store.HEAD]=store.encoded({'original':True});self.client.ack_lost=True
  with self.assertRaises(store.PublicationUncertain):self.publish(self.context)
  self.assertEqual(len(self.public_puts()),1);self.assertIn('metrics',json.loads(self.client.data[store.HEAD]))
 def test_denied_source_is_unknown_while_other_observed_counts_survive(self):
  self.client.errors[model.INPUTS['nyfed']]=Error('AccessDenied');packet,_=self.publish(self.context)
  rows={m['metric']:m for m in packet['metrics']}
  self.assertIsNone(rows['nyfed_reference_rates']['actual']);self.assertEqual(rows['us_tickers']['actual'],1)
  self.assertEqual(packet['sources']['nyfed']['read_status'],'source_denied')
 def test_malformed_complete_source_is_retained_without_becoming_measured_zero(self):
  raw=b'{"by_ticker":{},"by_ticker":{"TEST":{}}}';self.client.data[model.INPUTS['symbology']]=raw
  packet,_=self.publish(self.context)
  self.assertIsNone(packet['metrics'][0]['actual'])
  self.assertEqual(self.client.data[packet['sources']['symbology']['original_ref']['key']],raw)
 def test_short_body_is_closed_and_never_archived_as_whole(self):
  self.client.lengths[model.INPUTS['symbology']]=1;packet,_=self.publish(self.context)
  self.assertIsNone(packet['sources']['symbology']['original_ref']);self.assertIsNone(packet['metrics'][0]['actual'])
  self.assertTrue(all(body.closed for body in self.client.streams))
 def test_corrupt_gzip_source_stays_retained_and_unavailable(self):
  raw=b'\x1f\x8bINVALID-GZIP';self.client.data[model.INPUTS['symbology']]=raw
  self.client.encodings[model.INPUTS['symbology']]='gzip'
  packet,_=self.publish(self.context)
  self.assertIsNone(packet['metrics'][0]['actual'])
  self.assertEqual(self.client.data[packet['sources']['symbology']['original_ref']['key']],raw)
 def test_retained_collision_or_corruption_prevents_publication(self):
  raw=self.client.data[model.INPUTS['symbology']];key=store.PREFIX+'sources/'+hashlib.sha256(raw).hexdigest()+'.bin';self.client.data[key]=b'invented corrupt prior'
  with self.assertRaises(ValueError):self.publish(self.context)
  self.assertFalse(self.public_puts())
 def test_all_inputs_and_compiler_bytes_are_bound_and_projection_replays(self):
  packet,_=self.publish(self.context);manifest=json.loads(self.client.data[packet['replay']['input_ref']['key']]);docs={}
  for name,item in manifest['attempts'].items():docs[name]=store.strict(self.client.data[item['original_ref']['key']],item['content_encoding'])
  replay=model.project(manifest['attempts'],docs,manifest['generated_at'])
  for k,v in replay.items():self.assertEqual(packet[k],v)
  for name,path in self.compilers.items():self.assertEqual(self.client.data[manifest['source_files'][name]['key']],path.read_bytes())
 def test_missing_or_bad_clock_does_not_touch_storage(self):
  for context in (None,SimpleNamespace(get_remaining_time_in_millis=lambda:float('nan')),SimpleNamespace(get_remaining_time_in_millis=lambda:40000)):
   with self.assertRaises(ValueError):self.publish(context)
  self.assertFalse(self.client.reads);self.assertFalse(self.client.puts)
 def test_aggregate_limit_prevents_partial_population_publication(self):
  with patch.object(store,'MAX_TOTAL',1):
   with self.assertRaises(ValueError):self.publish(self.context)
  self.assertFalse(self.public_puts())
 def test_only_reviewed_keys_can_be_read(self):
  for key in ('data/private-account.json','audit-private/other/secret','data/not-declared.json'):
   with self.assertRaises(ValueError):store.read(self.client,'invented-bucket',key)
  self.assertFalse(self.client.reads)
 def test_strict_framing_accepts_whole_gzip_but_rejects_trailing_short_duplicate_and_nonfinite(self):
  import gzip
  raw=store.encoded({'complete':[1,None,'invented']});packed=gzip.compress(raw)
  self.assertEqual(store.strict(packed,'gzip'),json.loads(raw))
  for data,encoding in ((packed+b'extra','gzip'),(packed+packed,'gzip'),(packed[:-3],'gzip'),
       (packed,'identity'),(raw,'gzip'),(b'{"a":1,"a":2}',''),(b'{"a":NaN}',''),(b'{"a":1e999}','')):
   with self.assertRaises((ValueError,store.zlib.error)):
    store.strict(data,encoding)

if __name__=='__main__':unittest.main()
