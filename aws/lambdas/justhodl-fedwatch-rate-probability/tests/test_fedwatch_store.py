from fedwatch_fixture import *
from test_fedwatch_sources import calendar_raw,chart_doc,START,GENERATED
import importlib.util

class Store(unittest.TestCase):
 def test_retained_whole_originals_reproduce_the_real_publication(self):
  s,i,p=packet();read=store.reader(s,'b');out=store.replay(p['replay'],read)
  self.assertEqual(out,{k:v for k,v in p.items() if k!='replay'});self.assertEqual(len(out['contracts']),12)
  self.assertEqual(len(out['policy_measurements']),4)
  for key,ref in i['legacy'].items():self.assertEqual(read(ref['key']),s.objects[key])
  spec=importlib.util.spec_from_file_location('replay_native_fedwatch',ROOT/'scripts/replay_fedwatch_research.py');cli=importlib.util.module_from_spec(spec);spec.loader.exec_module(cli)
  self.assertTrue(cli.verify(p,read)['replayed'])
 def test_changed_provider_original_fails_hash_and_replay(self):
  s,i,p=packet();ref=i['collection']['quotes']['ZQV26.CBT']['original'];s.objects[ref['key']]=b'{}'
  with self.assertRaises(ValueError):store.replay(p['replay'],store.reader(s,'b'))
 def test_canonical_and_compiler_bytes_are_pinned(self):
  s,i,p=packet();run=json.loads(s.objects[p['replay']['manifest_key']]);ref=run['compilers']['fedwatch_research_model'];s.objects[ref['key']]+=b'\n'
  with self.assertRaises(ValueError):store.replay(p['replay'],store.reader(s,'b'))
  s,i,p=packet();key=next(k for k in s.objects if k.startswith('data/evidence/fred/'));s.objects[key]=gzip.compress(b'{}')
  with self.assertRaises(ValueError):store.replay(p['replay'],store.reader(s,'b'))
 def test_keyless_collection_stops_quotes_after_rate_limit(self):
  s=Storage();calls=[]
  def fetch(url,deadline):
   calls.append(url);return (200,'text/html',calendar_raw()) if url==model.CALENDAR_URL else (429,'application/json',b'{"error":"rate_limit"}')
  with patch.object(store,'now',return_value=START):c=store.collect(s,'b',fetch)
  self.assertEqual(len(calls),2);self.assertEqual(c['provider_requests'],2);self.assertEqual(len(c['quotes']),12)
  self.assertFalse(c['quotes']['ZQV26.CBT']['request_sent']);self.assertEqual(c['quotes']['ZQV26.CBT']['error'],'provider_access_or_rate_limit')
  for p in [c['calendar'],c['quotes']['ZQU26.CBT']]:self.assertEqual(s.objects[p['original']['key']].decode(),calendar_raw().decode() if p is c['calendar'] else '{"error":"rate_limit"}')
 def test_provider_failure_does_not_mask_original_retention_failure(self):
  s=Storage()
  def failed(*a):raise ValueError('Provider request failed')
  with patch.object(store,'now',return_value=START):c=store.collect(s,'b',failed)
  self.assertEqual(c['provider_requests'],13);self.assertEqual(c['source_bytes'],0);self.assertTrue(all(p['original'] is None for p in c['quotes'].values()))
  def denied(**kw):raise Failure('AccessDenied')
  s.put_object=denied
  with patch.object(store,'now',return_value=START):
   with self.assertRaises(Failure):store.collect(s,'b',lambda *a:(200,'text/html',calendar_raw()))
 def test_request_is_idempotent_and_failure_keeps_prior_current(self):
  s,i,_,_=fixture()
  with patch.object(store,'collect',return_value=i['collection']) as capture,patch.object(store,'now',return_value=GENERATED):
   result=store.run(s,'b','reviewed-run','aws-request');self.assertTrue(result['published'])
   self.assertEqual(result,store.run(s,'b','reviewed-run','different-aws-request'));self.assertEqual(capture.call_count,1)
  before=s.objects[store.CURRENT]
  with patch.object(store,'collect',side_effect=ValueError('sensitive failure text')):
   with self.assertRaises(RuntimeError):store.run(s,'b','failed-run','new-aws-request')
  self.assertEqual(s.objects[store.CURRENT],before)
  status=json.loads(s.objects[store.request_key('failed-run')]);self.assertEqual(status['status'],'failed');self.assertNotIn('sensitive',json.dumps(status))
 def test_newer_current_is_not_overwritten(self):
  s,i,p=packet();new=deepcopy(p);new['generated_at']='2026-09-20T23:55:00+00:00';s.objects[store.CURRENT]=model.encoded(new)
  self.assertFalse(store.publish(s,'b',p));self.assertEqual(json.loads(s.objects[store.CURRENT]),new)
 def test_read_paths_and_provider_urls_remain_restricted(self):
  for key in ('portfolio/state.json','data/fedwatch-research/../account.json','https://other.invalid/data.json'):
   self.assertFalse(store.allowed(key))
  for url in ('https://query2.finance.yahoo.com/v8/finance/chart/ZQV26.CBT?range=10d&interval=1d','https://other.invalid/?api_key=secret'):
   with self.assertRaises(ValueError):store.fetch(url,0)
 def test_http_is_read_only_and_validate_only_avoids_aws(self):
  import lambda_function as handler
  s,i,p=packet();s.objects[store.CURRENT]=model.encoded(p)
  with patch.object(handler.boto3,'client',return_value=s),patch.object(handler,'run',side_effect=AssertionError('HTTP must not publish')):
   response=handler.lambda_handler({'httpMethod':'GET'});self.assertEqual(json.loads(response['body']),p)
  with patch.object(handler.boto3,'client',side_effect=AssertionError('Validation must not call AWS')):
   self.assertEqual(handler.lambda_handler({'validate_only':True})['statusCode'],200)

if __name__=='__main__':unittest.main(verbosity=2)
