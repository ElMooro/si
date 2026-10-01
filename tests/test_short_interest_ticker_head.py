from pathlib import Path
from io import BytesIO,StringIO
from unittest.mock import Mock
import ast,contextlib,copy,hashlib,json,re,sys,time,types,unittest
from datetime import date
R=Path(__file__).resolve().parents[1];W=R
sys.path[:0]=[str(R/'aws/shared'),str(Path(__file__).parent)]
import short_interest_research_model as model
import short_interest_research_store as store
from test_short_interest_ticker_identity import record,t

class Head(unittest.TestCase):
 def setup(self):
  rec=record();identifier=model.record_id(tuple(rec['identity'][k] for k in t.GRAIN));prefix=identifier[:2]
  shard={'contract':'short-interest-record-shard.v1','records':{identifier:rec}}
  raw=model.encoded(shard);ref=store.identity(raw,'records')
  packet={'contract':model.CONTRACT,'record_shards':{prefix:ref},'counts':{'issues':1},
          'settlement_date':'2026-09-15','generated_at':'2026-10-01T00:00:00Z',**model.PERMISSIONS}
  replay={'manifest_key':model.PREFIX+'runs/'+'a'*64+'.json','output_sha256':model.digest(packet)}
  packet['replay']=replay;self.packet=packet;self.result={'published':True,'replay':copy.deepcopy(replay)};self.raw=raw;self.prefix=prefix;self.ref=ref
  self.publication=Mock(return_value={'n_tickers':1,'settlement_date':'2026-09-15'});self.boto=Mock();self.client=Mock();self.client.get_object.side_effect=self.get
  source=(R/'aws/lambdas/justhodl-short-interest/source/lambda_function.py').read_text(encoding='utf-8');tree=ast.parse(source);fn=next(n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name=='_publish_tickers_layer')
  scope={'model':model,'store':store,'re':re,'time':time,'boto3':self.boto,'Config':lambda **kw:kw,'date':date,
         'tickers':types.SimpleNamespace(GRAIN=t.GRAIN,TICKERS_KEY=t.TICKERS_KEY,publish_tickers_artifact=self.publication),'PUBLISHED_KEY':'data/short-interest.json','__file__':str((R/'aws/lambdas/justhodl-short-interest/source/lambda_function.py')), 'producer':types.SimpleNamespace(__name__='short_interest_producer',__file__=str(R/'aws/shared/short_interest_producer.py')), 'short_interest_context':types.SimpleNamespace(__name__='short_interest_context',__file__=str(R/'aws/shared/short_interest_context.py'))}
  exec(compile(ast.Module(body=[fn],type_ignores=[]),'actual_candidate_handler','exec'),scope);self.raw_call=scope[fn.name];self.call=lambda *a:self.raw_call(*a,remaining_seconds=360)
 def get(self,**kw):
  if kw['Key']=='data/short-interest.json':return {'Body':BytesIO(model.encoded(self.packet))}
  if kw['Key']==self.ref['key']:return {'Body':BytesIO(self.raw)}
  raise AssertionError('Unreviewed object request')
 def resign(self):
  self.packet['replay']['output_sha256']=model.digest({k:v for k,v in self.packet.items() if k!='replay'});self.result['replay']=copy.deepcopy(self.packet['replay'])
 def reject(self):
  with self.assertRaises((ValueError,TypeError)):self.call(self.client,'invented',self.result)
  self.boto.client.assert_not_called();self.publication.assert_not_called()
 def test_invalid_or_insufficient_time_never_reads_storage(self):
  for value in [None,True,119,-1,float('nan'),float('inf')]:
   self.setup()
   with self.assertRaises(ValueError):self.raw_call(self.client,'invented',self.result,remaining_seconds=value)
   self.client.get_object.assert_not_called();self.boto.client.assert_not_called();self.publication.assert_not_called()
 def test_whole_matching_head_and_shard_accepted(self):
  self.setup()
  with contextlib.redirect_stdout(StringIO()):self.call(self.client,'invented',self.result)
  self.assertEqual(self.client.get_object.call_count,2);self.publication.assert_called_once()
 def test_unsuccessful_producer_never_reads_or_publishes(self):
  self.setup();self.result['published']=False;self.assertIsNone(self.call(self.client,'invented',self.result));self.client.get_object.assert_not_called();self.boto.client.assert_not_called()
 def test_changed_current_run_cannot_be_selected_after_race(self):
  self.setup();self.packet['replay']['manifest_key']=model.PREFIX+'runs/'+'b'*64+'.json';self.reject()
 def test_altered_head_body_is_rejected_even_with_same_replay(self):
  self.setup();self.packet['settlement_date']='2026-08-31';self.reject()
 def test_wrong_contract_and_qualified_authority_rejected(self):
  for key,value in [('contract','other'),('calls_eligible',True),('forecast_qualified',None)]:
   self.setup();self.packet[key]=value;self.resign();self.reject()
 def test_wrong_shard_namespace_rejected_before_object_read(self):
  self.setup();self.packet['record_shards'][self.prefix]['key']='private/account.json';self.resign();self.reject();self.assertEqual(self.client.get_object.call_count,1)
 def test_corrupt_received_shard_fails_hash_check(self):
  self.setup();self.raw+=b' ';self.reject()
 def test_boolean_and_wrong_byte_counts_rejected(self):
  for value in [True,0,self.ref_bytes()+1]:
   self.setup();self.packet['record_shards'][self.prefix]['bytes']=value;self.resign();self.reject()
 def ref_bytes(self):self.setup();return self.ref['bytes']
 def test_wrong_prefix_cannot_reassign_an_issue(self):
  self.setup();wrong='ff' if self.prefix!='ff' else 'ee';self.packet['record_shards']={wrong:self.ref};self.resign();self.reject()
 def test_wrong_and_boolean_head_inventory_counts_rejected(self):
  for value in [0,2,True]:
   self.setup();self.packet['counts']['issues']=value;self.resign();self.reject()
 def test_changed_issue_identity_rejected_with_self_consistent_shard_hash(self):
  self.setup();s=model.strict(self.raw);next(iter(s['records'].values()))['identity']['issueName']='Other'
  self.raw=model.encoded(s);self.ref=store.identity(self.raw,'records');self.packet['record_shards'][self.prefix]=self.ref;self.resign();self.reject()
 def test_duplicate_issue_cannot_reappear_in_second_shard(self):
  self.setup();wrong='ff' if self.prefix!='ff' else 'ee';self.packet['record_shards'][wrong]=copy.deepcopy(self.ref);self.resign();self.reject()

if __name__=='__main__':unittest.main(verbosity=2)
