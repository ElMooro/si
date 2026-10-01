from pathlib import Path
from io import StringIO
from unittest.mock import patch
import contextlib,hashlib,importlib.util,json,sys,types,unittest
from test_short_interest_ticker_identity import record,t,W,R
sys.path.insert(0,str(R/'tests'))
from synthesis_status_test_support import Memory,Error,store
import short_interest_research_model as model
import short_interest_research_store as research_store

class Publication(unittest.TestCase):
 def setUp(self):
  rec=record();identifier=model.record_id(tuple(rec['identity'][k] for k in t.GRAIN));self.prefix=identifier[:2]
  self.shards={self.prefix:{'contract':'short-interest-record-shard.v1','records':{identifier:rec}}}
  self.packet={'contract':model.CONTRACT,'generated_at':'2026-10-01T00:00:00Z','settlement_date':'2026-09-15',
       'record_shards':{self.prefix:research_store.identity(model.encoded(self.shards[self.prefix]),'records')},'counts':{'issues':1},**model.PERMISSIONS}
  self.packet['replay']={'manifest_key':model.PREFIX+'runs/'+'a'*64+'.json','output_sha256':model.digest(self.packet)}
  self.raw=model.encoded(self.packet);self.mem=Memory();self.mem.head=t.TICKERS_KEY;self.mem.inputs={model.CURRENT};self.mem.prefix='audit-private/20260909-originals/short-interest-ticker-projection/'
  self.mem.objects[model.CURRENT]=self.raw
 def run_publish(self):
  return t.publish_tickers_artifact(self.mem,'justhodl-dashboard-live',self.shards,self.packet['settlement_date'],self.packet['generated_at'],expected_head=self.raw,compiler_paths={'compiler.py':Path(__file__)})
 def head(self):return json.loads(self.mem.objects[self.mem.head])
 def test_complete_head_compilers_previous_and_output_are_retained_before_conditional_write(self):
  previous=b'{"legacy":"invented"}';self.mem.objects[self.mem.head]=previous;self.run_publish();head=self.head()
  self.assertEqual(self.mem.objects[head['replay']['previous_publication']['key']],previous)
  manifest=json.loads(self.mem.objects[head['replay']['input_ref']['key']]);ref=manifest['attempts']['research']['original_ref']
  self.assertEqual(self.mem.objects[ref['key']],self.raw);self.assertEqual(len(manifest['source_files']),3)
  self.assertTrue(all(self.mem.objects[ref['key']] for ref in manifest['source_files'].values()))
  self.assertEqual(head['source_generated_at'],self.packet['generated_at']);self.assertNotEqual(head['generated_at'],head['source_generated_at'])
  self.assertTrue(all(head[k] is False for k in model.PERMISSIONS));self.assertEqual(head['independent_investment_votes'],0)
  self.assertIn('IfMatch',self.mem.writes[-1]);self.assertEqual(self.mem.writes[-1]['CacheControl'],'no-store')
  self.assertTrue(all(stream.closed for stream in self.mem.streams))
  self.assertTrue(any(k.startswith(self.mem.prefix+'outputs/') and raw==self.mem.objects[self.mem.head] for k,raw in self.mem.objects.items()))
 def test_complete_projection_replays_from_retained_input_clock_and_canonical_shards(self):
  self.run_publish();head=self.head();manifest=json.loads(self.mem.objects[head['replay']['input_ref']['key']])
  source=self.mem.objects[manifest['attempts']['research']['original_ref']['key']]
  rebuilt_shards={}
  for prefix,ref in model.strict(source)['record_shards'].items():
   original=model.encoded(self.shards[prefix]);self.assertEqual(research_store.identity(original,'records'),ref)
   rebuilt_shards[prefix]=model.strict(original)
  rebuilt=t.project_verified_tickers(rebuilt_shards,source,manifest['generated_at'])
  self.assertEqual(rebuilt,{k:v for k,v in head.items() if k not in ('replay','acquisition_started_at')})
 def test_missing_previous_head_uses_create_only(self):
  self.run_publish();self.assertEqual(self.mem.writes[-1]['IfNoneMatch'],'*')
 def test_actual_typed_consumer_accepts_zero_without_granting_investment_authority(self):
  import short_position_context
  identifier=next(iter(self.shards[self.prefix]['records']))
  self.shards[self.prefix]['records'][identifier]=record(quantity=0)
  self.packet['record_shards']={k:research_store.identity(model.encoded(v),'records') for k,v in self.shards.items()}
  self.packet['replay']['output_sha256']=model.digest({k:v for k,v in self.packet.items() if k!='replay'})
  self.raw=model.encoded(self.packet);self.mem.objects[model.CURRENT]=self.raw;self.run_publish()
  consumer=short_position_context.descriptive_context(self.head())
  self.assertEqual(consumer['by_ticker']['TEST']['short_interest_shares'],0)
  self.assertIs(consumer['by_ticker']['TEST']['latest_reported'],True)
  self.assertEqual(consumer['independent_investment_votes'],0)
  self.assertTrue(all(consumer[k] is False for k in model.PERMISSIONS))
 def test_changed_canonical_head_aborts_instead_of_overwriting_newer_projection(self):
  self.mem.objects[self.mem.head]=b'newer';self.mem.objects[model.CURRENT]=self.raw+b' '
  with self.assertRaisesRegex(ValueError,'changed or unavailable'):self.run_publish()
  self.assertEqual(self.mem.objects[self.mem.head],b'newer');self.assertFalse(any(w['Key']==self.mem.head for w in self.mem.writes))
 def test_competing_writer_is_not_retried(self):
  self.mem.race=b'competitor'
  with self.assertRaises(store.PublicationUncertain):self.run_publish()
  self.assertEqual(self.mem.objects[self.mem.head],b'competitor');self.assertEqual(sum(w['Key']==self.mem.head for w in self.mem.writes),1)
 def test_uncertain_acknowledgement_does_not_claim_rollback_or_retry(self):
  self.mem.fail=('ack',self.mem.head)
  with self.assertRaises(store.PublicationUncertain):self.run_publish()
  self.assertEqual(self.head()['contract'],'short-interest-tickers.v1');self.assertEqual(sum(w['Key']==self.mem.head for w in self.mem.writes),1)
 def test_source_retention_failure_preserves_previous(self):
  self.mem.objects[self.mem.head]=b'old';self.mem.fail=('put_kind','sources')
  with self.assertRaises(Error):self.run_publish()
  self.assertEqual(self.mem.objects[self.mem.head],b'old')
 def test_denied_prior_is_not_absence(self):
  self.mem.fail=('get',self.mem.head)
  with self.assertRaises(Error):self.run_publish()
  self.assertEqual(self.mem.writes,[])
 def test_shard_byte_identity_must_match_before_any_storage_access(self):
  self.shards[self.prefix]['extra']='changed'
  with self.assertRaises(ValueError):self.run_publish()
  self.assertEqual(self.mem.reads,[]);self.assertEqual(self.mem.writes,[])
 def test_publication_retains_all_ambiguous_occurrences(self):
  rec=record(name='Invented Class B');identifier=model.record_id(tuple(rec['identity'][k] for k in t.GRAIN))
  self.shards.setdefault(identifier[:2],{'contract':'short-interest-record-shard.v1','records':{}})['records'][identifier]=rec
  self.packet['record_shards']={k:research_store.identity(model.encoded(v),'records') for k,v in self.shards.items()};self.packet['counts']['issues']=2
  self.packet['replay']['output_sha256']=model.digest({k:v for k,v in self.packet.items() if k!='replay'})
  self.raw=model.encoded(self.packet);self.mem.objects[model.CURRENT]=self.raw;self.run_publish();head=self.head()
  self.assertEqual(head['by_ticker'],{});self.assertEqual(len(head['identity_report']['ambiguous_symbols'][0]['occurrences']),2)
 def test_forged_replay_or_authority_is_rejected_before_storage(self):
  for key,value in [('calls_eligible',True),('generated_at','invalid-clock')]:
   self.setUp();self.packet[key]=value;self.raw=model.encoded(self.packet)
   with self.assertRaises(ValueError):self.run_publish()
   self.assertEqual(self.mem.reads,[]);self.assertEqual(self.mem.writes,[])
  self.setUp();self.packet['replay']['output_sha256']='b'*64;self.raw=model.encoded(self.packet)
  with self.assertRaises(ValueError):self.run_publish()
  self.assertEqual(self.mem.writes,[])
 def load_native(self):
  fake=types.ModuleType('boto3')
  def client(name,**kwargs):self.assertEqual(name,'s3');self.mem.clients.append(kwargs);return self.mem
  fake.client=client;config=types.ModuleType('botocore.config');config.Config=lambda **kwargs:kwargs
  self.modules={'boto3':fake,'botocore.config':config,'short_interest_tickers':t}
  spec=importlib.util.spec_from_file_location('whole_short_interest_candidate',(R/'aws/lambdas/justhodl-short-interest/source/lambda_function.py'));native=importlib.util.module_from_spec(spec)
  with patch.dict(sys.modules,self.modules):spec.loader.exec_module(native)
  for prefix,shard in self.shards.items():
   key=self.packet['record_shards'][prefix]['key'];self.mem.inputs.add(key);self.mem.objects[key]=model.encoded(shard)
  return native
 def test_whole_native_handler_publishes_retained_projection_without_provider_or_socket(self):
  native=self.load_native();context=types.SimpleNamespace(aws_request_id='invented',get_remaining_time_in_millis=lambda:360000)
  result={'published':True,'replay':self.packet['replay']}
  def deny(*a,**kw):raise AssertionError('No provider/network')
  with patch.dict(sys.modules,self.modules),patch.object(native.producer,'run',return_value=result),patch('urllib.request.urlopen',deny),patch('socket.create_connection',deny),contextlib.redirect_stdout(StringIO()):
   response=native.lambda_handler({},context)
  self.assertEqual(json.loads(response['body'])['consumer_projection'],{'status':'published'})
  head=self.head();self.assertEqual(len(head['replay']['source_files']),9);self.assertEqual(len(self.mem.clients),2)
  self.assertFalse(any(k not in self.mem.inputs and k!=self.mem.head and not k.startswith(self.mem.prefix) for k in self.mem.reads))
 def test_whole_native_defers_projection_when_remaining_time_is_short(self):
  native=self.load_native();context=types.SimpleNamespace(aws_request_id='invented',get_remaining_time_in_millis=lambda:60000)
  with patch.dict(sys.modules,self.modules),patch.object(native.producer,'run',return_value={'published':True,'replay':self.packet['replay']}),contextlib.redirect_stdout(StringIO()):response=native.lambda_handler({},context)
  self.assertEqual(json.loads(response['body'])['consumer_projection'],{'status':'unavailable','error_type':'ValueError'});self.assertEqual(self.mem.reads,[]);self.assertEqual(self.mem.writes,[])
 def test_whole_native_exposes_projection_failure_without_error_body_or_false_success(self):
  native=self.load_native();context=types.SimpleNamespace(aws_request_id='invented',get_remaining_time_in_millis=lambda:360000);self.mem.fail=('get',self.mem.head)
  with patch.dict(sys.modules,self.modules),patch.object(native.producer,'run',return_value={'published':True,'replay':self.packet['replay']}),contextlib.redirect_stdout(StringIO()):response=native.lambda_handler({},context)
  self.assertEqual(json.loads(response['body'])['consumer_projection'],{'status':'unavailable','error_type':'Error'});self.assertNotIn(self.mem.head,self.mem.objects)

if __name__=='__main__':unittest.main(verbosity=2)
