from pathlib import Path
import copy,io,json,runpy,types,unittest
R=Path(__file__).resolve().parents[1]
M=types.SimpleNamespace(**runpy.run_path(str(R/'aws/shared/crypto_funding_observations.py')))
import sys
sys.path.insert(0,str(R/'aws/shared'))
A=runpy.run_path(str(R/'aws/shared/crypto_funding_archive.py'))
F=runpy.run_path(str(R/'tests/test_crypto_funding_observations.py'))

class StorageFailure(Exception):
 def __init__(self,code):self.response={'Error':{'Code':code}}

class Store:
 def __init__(self):self.data={};self.calls=[];self.corrupt=False;self.short=False
 def put_object(self,**kw):
  self.calls.append(('put',kw['Key']));assert kw['IfNoneMatch']=='*'
  if kw['Key'] in self.data:raise StorageFailure('412')
  self.data[kw['Key']]=kw['Body']
 def get_object(self,**kw):
  self.calls.append(('get',kw['Key']));raw=self.data[kw['Key']]
  if self.corrupt:raw+=b'corrupt'
  class Short(io.BytesIO):
   def read(self,n):return super().read(min(n,3))
  return {'Body':Short(raw) if self.short else io.BytesIO(raw),'ContentLength':len(raw)}

def packet(source=None):return M.collect_funding(F['transport_factory'](only=source))
def compilers():return {'crypto_funding_observations.py':(R/'aws/shared/crypto_funding_observations.py').read_bytes(),'crypto_funding_archive.py':(R/'aws/shared/crypto_funding_archive.py').read_bytes()}

class Archive(unittest.TestCase):
 def test_complete_event_and_fallback_ledgers_replay_from_originals(self):
  for source in [None,'bybit']:
   p=packet(source);self.assertEqual(A['replay_capture'](p,M),p)
 def test_incomplete_and_failed_source_ledgers_remain_explicit(self):
  from unittest.mock import Mock
  p=M.collect_funding(Mock(side_effect=TimeoutError('invented')))
  self.assertEqual(A['replay_capture'](p,M),p);self.assertEqual(p['observed_instruments'],0)
 def test_changed_original_hash_rate_clock_coverage_and_permission_are_rejected(self):
  p=packet()
  for update in [lambda d:d.update(observed_instruments=9),lambda d:d.update(calls_eligible=True),lambda d:d['rates'][0].update(funding_rate_pct=7),lambda d:d['source_attempts'][0].update(original_response_sha256='0'*64),lambda d:d['source_attempts'][0].update(request_url='https://other.invalid'),lambda d:d['source_attempts'][0].update(acquired_started_at='9999-01-01T00:00:00+00:00')]:
   bad=copy.deepcopy(p);update(bad)
   with self.assertRaises(ValueError):A['replay_capture'](bad,M)
 def test_missing_extra_or_reordered_source_attempts_rejected(self):
  p=packet()
  for attempts in [p['source_attempts'][:-1],p['source_attempts']*2,list(reversed(p['source_attempts']))]:
   bad=copy.deepcopy(p);bad['source_attempts']=attempts
   with self.assertRaises(ValueError):A['replay_capture'](bad,M)
 def test_content_addressed_retention_has_no_mutable_head_and_is_idempotent(self):
  client=Store();p=packet();refs=A['retain_checked'](client,'invented',p,M,compilers());before=copy.deepcopy(client.data)
  self.assertEqual(refs,A['retain_checked'](client,'invented',p,M,compilers()));self.assertEqual(client.data,before)
  self.assertEqual(len(client.data),4);self.assertTrue(all('/latest' not in key and key.startswith(A['PREFIX']) for key in client.data));self.assertFalse(refs['investment_authority'])
 def test_short_storage_chunks_are_not_mistaken_for_eof(self):
  client=Store();client.short=True;out=A['retain_checked'](client,'invented',packet(),M,compilers());self.assertTrue(out['complete_capture_replayed'])
 def test_corrupt_readback_prevents_any_acceptance_receipt(self):
  client=Store();client.corrupt=True
  with self.assertRaises(ValueError):A['retain_checked'](client,'invented',packet(),M,compilers())
 def test_reference_cannot_read_current_private_or_foreign_keys(self):
  client=Store();ref=A['reference']('captures',b'invented')
  for key in ['crypto-intel.json','portfolio/account.json','audit-private/anything',A['PREFIX']+'runs/'+ref['sha256']+'.bin']:
   with self.assertRaises(ValueError):A['complete_read'](client,'invented',dict(ref,key=key),'captures')
  self.assertEqual(client.calls,[])
 def test_write_uncertainty_propagates_without_duplicate_publication(self):
  from unittest.mock import Mock
  client=Store();client.put_object=Mock(side_effect=TimeoutError('invented'))
  with self.assertRaises(TimeoutError):A['retain_checked'](client,'invented',packet(),M,compilers())
  self.assertEqual(client.put_object.call_count,1)
 def test_replay_does_not_mutate_the_collected_packet(self):
  p=packet();before=copy.deepcopy(p);A['retain_checked'](Store(),'invented',p,M,compilers());self.assertEqual(p,before)


class PublishedStore(unittest.TestCase):
 def test_public_entrypoint_replays_with_exact_local_compilers(self):
  p=packet();client=Store();ref=A['retain'](client,'invented',p)
  self.assertEqual(A['replay'](client,'invented',ref['manifest']),p);self.assertFalse(ref['investment_authority'])
 def test_replay_has_no_storage_writes(self):
  p=packet();client=Store();ref=A['retain'](client,'invented',p);client.calls=[]
  A['replay'](client,'invented',ref['manifest']);self.assertTrue(client.calls);self.assertTrue(all(kind=='get' for kind,_ in client.calls))
 def test_matching_hash_does_not_allow_unreviewed_compiler_execution(self):
  p=packet();client=Store();ref=A['retain'](client,'invented',p)
  manifest=A['strict'](A['complete_read'](client,'invented',ref['manifest'],'runs'))
  foreign=b'raise RuntimeError("invented downloaded code must never run")'
  manifest['compilers']['crypto_funding_archive.py']=A['retain_bytes'](client,'invented','compilers',foreign)
  bad=A['retain_bytes'](client,'invented','runs',A['encode'](manifest))
  with self.assertRaisesRegex(ValueError,'Compiler differs'):A['replay'](client,'invented',bad)
 def test_manifest_cannot_promote_permissions_or_change_clocks(self):
  for key,value in [('calls_eligible',True),('point_in_time_qualified',True),('acquisition_completed_at','2020-01-01T00:00:00+00:00')]:
   p=packet();client=Store();ref=A['retain'](client,'invented',p);manifest=A['strict'](A['complete_read'](client,'invented',ref['manifest'],'runs'));manifest[key]=value
   bad=A['retain_bytes'](client,'invented','runs',A['encode'](manifest))
   with self.assertRaises(ValueError):A['replay'](client,'invented',bad)
 def test_missing_compiler_is_not_silently_skipped(self):
  p=packet();client=Store();ref=A['retain'](client,'invented',p);manifest=A['strict'](A['complete_read'](client,'invented',ref['manifest'],'runs'));manifest['compilers'].pop('crypto_funding_observations.py');bad=A['retain_bytes'](client,'invented','runs',A['encode'](manifest))
  with self.assertRaises(ValueError):A['replay'](client,'invented',bad)
 def test_zero_can_be_reproduced_without_current_packets(self):
  p=M.collect_funding(F['transport_factory']('0'));client=Store();ref=A['retain'](client,'invented',p);out=A['replay'](client,'invented',ref['manifest'])
  self.assertEqual(out['rates'][0]['funding_rate'],0);self.assertIsNone(out['avg_funding']);self.assertTrue(all(key.startswith(A['PREFIX']) for _,key in client.calls))

class Publication(unittest.TestCase):
 def test_actual_collector_publishes_whole_original_after_replay(self):
  from unittest.mock import Mock,patch
  client=Store();client.close=Mock();opener=F['transport_factory']('0')
  out=A['collect_retained'](opener,lambda:client,'invented',lambda:180000)
  self.assertEqual(opener.call_count,10);client.close.assert_called_once()
  proof=out.pop('original_capture');self.assertEqual(A['replay'](client,'invented',proof['manifest']),out)
  self.assertEqual(out['rates'][0]['funding_rate'],0);self.assertFalse(out['calls_eligible'])
 def test_storage_failures_cannot_publish_collected_values_or_leak_errors(self):
  from unittest.mock import Mock
  for stage in ('put_object','get_object'):
   client=Store();client.close=Mock();setattr(client,stage,Mock(side_effect=RuntimeError('https://invented.invalid/?token=PRIVATE')))
   with self.assertRaisesRegex(ValueError,'descriptive publication withheld') as error:
    A['collect_retained'](F['transport_factory'](),lambda:client,'invented',lambda:180000)
   self.assertNotIn('PRIVATE',str(error.exception));client.close.assert_called_once()
 def test_invalid_or_short_budget_makes_no_storage_requests(self):
  from unittest.mock import Mock
  for remaining in [0,29999,None,True,30000.0,'180000']:
   factory=Mock()
   with self.assertRaises(ValueError):A['collect_retained'](F['transport_factory'](),factory,'invented',lambda:remaining)
   factory.assert_not_called()
 def test_expiring_budget_stops_before_new_operation(self):
  from unittest.mock import Mock
  client=Store();client.close=Mock();budget=Mock(side_effect=[180000,180000,0])
  with self.assertRaises(ValueError):A['collect_retained'](F['transport_factory'](),lambda:client,'invented',budget)
  self.assertEqual([kind for kind,_ in client.calls],['put']);client.close.assert_called_once()
 def test_budget_checked_on_body_chunks_and_stream_closed_on_failure(self):
  from unittest.mock import Mock
  raw=b'invented';ref=A['reference']('captures',raw);stream=io.BytesIO(raw)
  client=types.SimpleNamespace(get_object=Mock(return_value={'Body':stream,'ContentLength':len(raw)}))
  guard=A['BudgetStore'](client,Mock(side_effect=[180000,0]))
  with self.assertRaises(ValueError):A['complete_read'](guard,'invented',ref,'captures')
  self.assertTrue(stream.closed)
 def test_sdk_client_has_timeouts_and_no_automatic_retry(self):
  from unittest.mock import Mock,patch
  factory=Mock();config=Mock(side_effect=lambda **kw:kw)
  with patch.dict(sys.modules,{'boto3':types.SimpleNamespace(client=factory),'botocore.config':types.SimpleNamespace(Config=config)}):A['storage_client']()
  factory.assert_called_once_with('s3',config={'connect_timeout':2,'read_timeout':3,'retries':{'total_max_attempts':1}})
 def test_strict_parser_rejects_nonfinite_overflow_duplicates_and_bad_utf8(self):
  for raw in [b'{"v":1e999}',b'{"v":NaN}',b'{"v":1,"v":2}',b'\xff',b'']:
   with self.assertRaises(ValueError):A['strict'](raw)
 def test_wrong_retention_assurance_refused(self):
  client=Store();ref=A['retain'](client,'invented',packet());manifest=A['strict'](A['complete_read'](client,'invented',ref['manifest'],'runs'));manifest['retention_mode']='object_lock'
  bad=A['retain_bytes'](client,'invented','runs',A['encode'](manifest))
  with self.assertRaises(ValueError):A['replay'](client,'invented',bad)
 def test_complete_file_and_predecessor_manifest_changes_are_retained(self):
  d=R/'tests/fixtures/crypto-funding-archive';plans=json.loads((d/'edits.json').read_bytes())
  for path,plan in plans.items():
   raw=(d/('before/'+path+'.txt')).read_bytes();self.assertEqual(A['sha'](raw),plan['predecessor_sha256']);text=raw.decode('utf-8')
   for before,after in plan['edits']:self.assertEqual(text.count(before),1,path);text=text.replace(before,after)
   self.assertEqual(text.encode(),(R/path).read_bytes(),path);self.assertEqual(A['sha'](text.encode()),plan['candidate_sha256'])
 def test_handler_passes_its_deadline_to_funding_collector(self):
  import ast
  tree=ast.parse((R/'aws/lambdas/justhodl-crypto-intel/source/lambda_function.py').read_text(encoding='utf-8'))
  calls=[n for n in ast.walk(tree) if isinstance(n,ast.Call) and isinstance(n.func,ast.Attribute) and n.func.attr=='submit' and n.args and isinstance(n.args[0],ast.Name) and n.args[0].id=='fetch_funding']
  self.assertEqual(len(calls),1);self.assertEqual(len(calls[0].args),2);self.assertEqual(calls[0].args[1].id,'context')

if __name__=='__main__':unittest.main(verbosity=2)


