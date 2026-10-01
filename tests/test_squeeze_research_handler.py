from pathlib import Path
from unittest.mock import patch
import gzip,importlib.util,json,sys,tempfile,types,unittest
R=Path(__file__).resolve().parents[1];W=R;sys.path[:0]=[str(R/'tests'),str(R/'aws/shared')]
from synthesis_status_test_support import Memory,Error,store
def deny(*args,**kwargs):raise AssertionError('No provider, key, model or socket request')
class Whole(unittest.TestCase):
 def setUp(self):
  self.tmp=tempfile.TemporaryDirectory(prefix='squeeze-candidate-',dir=None);self.here=Path(self.tmp.name);self.addCleanup(self.tmp.cleanup)
  for a,b in [('aws/lambdas/justhodl-squeeze-pretrigger/source/lambda_function.py','lambda_function.py'),('aws/lambdas/justhodl-squeeze-pretrigger/source/squeeze_research_model.py','squeeze_research_model.py')]: (self.here/b).write_bytes((W/a).read_bytes())
  self.mem=Memory();self.mem.head='data/squeeze-pretrigger.json';self.mem.inputs={'data/finra-short.json','data/short-interest-tickers.json','data/catalyst-calendar.json'};self.mem.prefix='audit-private/20260909-originals/squeeze-pretrigger-research/'
  self.fake=types.ModuleType('boto3')
  def client(name,**kw):self.assertEqual(name,'s3');self.mem.clients.append(kw);return self.mem
  self.fake.client=client;cfg=types.ModuleType('botocore.config');cfg.Config=lambda **kw:kw
  self.modules={'boto3':self.fake,'botocore.config':cfg,'managed_secret':types.SimpleNamespace(managed_secret=deny)}
  spec=importlib.util.spec_from_file_location('squeeze_research_model',self.here/'squeeze_research_model.py');self.model=importlib.util.module_from_spec(spec);spec.loader.exec_module(self.model);self.modules['squeeze_research_model']=self.model
  spec=importlib.util.spec_from_file_location('whole_squeeze',self.here/'lambda_function.py');self.mod=importlib.util.module_from_spec(spec)
  with patch.dict(sys.modules,self.modules),patch('urllib.request.urlopen',deny),patch('socket.create_connection',deny):spec.loader.exec_module(self.mod)
  self.assertEqual(self.mem.clients,[])
 def run_handler(self):
  with patch.dict(sys.modules,self.modules),patch('urllib.request.urlopen',deny),patch('socket.create_connection',deny):
   return self.mod.lambda_handler({'legacy':True,'model':'paid','source':'private/account.json','output':'private/account.json'},None)
 def packet(self):return json.loads(self.mem.objects[self.mem.head])
 def fill(self):
  self.mem.objects.update({'data/finra-short.json':b'{"calls_eligible":true,"tickers":{"TEST":{"short_utilization":99}}}',
   'data/short-interest-tickers.json':b'{"contract":"short-interest-tickers.v1","by_ticker":{"TEST":{"short_interest":0}}}',
   'data/catalyst-calendar.json':b'{"events":[{"ticker":"TEST","date":"2026-10-01","text":"invented"}]}'})
 def safe(self):
  p=self.packet();self.assertEqual(p['state'],'UNQUALIFIED');self.assertEqual(p['portfolio_action'],'WAIT');self.assertIsNone(p['recommended_trade']);self.assertTrue(all(p[k] is False for k in self.model.FLAGS));self.assertEqual(self.mem.writes[-1]['Key'],self.mem.head);self.assertTrue(all(s.closed for s in self.mem.streams));self.assertEqual(len(self.mem.clients),1);return p
 def test_imports_and_disabled_legacy_never_resolve_keys_or_call_clients(self):
  self.assertIsNone(self.mod.FMP_KEY);self.assertIsNone(self.mod.TELEGRAM_TOKEN)
  with self.assertRaisesRegex(RuntimeError,'disabled'):self.mod._legacy_lambda_handler({},None)
  self.assertEqual(self.mem.clients,[]);self.assertEqual(self.mem.reads,[]);self.assertEqual(self.mem.writes,[])
 def test_all_missing_keeps_wait_and_never_makes_no_setups_claim(self):
  self.assertEqual(self.run_handler()['statusCode'],200);p=self.safe();self.assertIsNone(p['summary']['n_total_setups']);self.assertEqual(set(self.mem.inputs),set(k for k in self.mem.reads if k in self.mem.inputs));self.assertEqual(self.mem.clients[0]['config'],{'connect_timeout':2,'read_timeout':3,'retries':{'max_attempts':0}})
 def test_complete_retained_originals_compilers_and_replay_before_publication(self):
  self.fill();self.run_handler();p=self.safe();manifest=json.loads(self.mem.objects[p['replay']['input_ref']['key']]);self.assertEqual(set(manifest['attempts']),set(self.model.INPUTS));self.assertEqual(len(manifest['source_files']),5)
  sources={a['original_ref']['key']:self.mem.objects[a['original_ref']['key']] for a in manifest['attempts'].values()}
  for name,a in manifest['attempts'].items():self.assertEqual(sources[a['original_ref']['key']],self.mem.objects[self.model.INPUTS[name]])
  for name,ref in manifest['source_files'].items():self.assertTrue(self.mem.objects[ref['key']]);self.assertTrue(ref['key'].startswith(self.mem.prefix+'compilers/'))
  rebuilt=self.model.project(manifest['attempts'],sources,manifest['generated_at'],store.validate_ref,self.mem.prefix);self.assertEqual(rebuilt,{k:v for k,v in p.items() if k not in ('replay','acquisition_started_at')})
 def test_previous_complete_legacy_publication_is_preserved(self):
  prior=b'{"legacy":"invented","forward_expectations":{"wr":99}}';self.mem.objects[self.mem.head]=prior;self.run_handler();p=self.safe();self.assertEqual(self.mem.objects[p['replay']['previous_publication']['key']],prior);self.assertIn('IfMatch',self.mem.writes[-1])
 def test_source_retention_failure_never_replaces_head(self):
  self.fill();prior=b'previous';self.mem.objects[self.mem.head]=prior;self.mem.fail=('put_kind','sources')
  with self.assertRaises(Error):self.run_handler()
  self.assertEqual(self.mem.objects[self.mem.head],prior);self.assertFalse(any(w['Key']==self.mem.head for w in self.mem.writes))
 def test_denied_prior_is_not_absence(self):
  self.mem.fail=('get',self.mem.head)
  with self.assertRaises(Error):self.run_handler()
  self.assertEqual(self.mem.writes,[])
 def test_competing_publication_is_not_silently_retried_or_overwritten(self):
  self.mem.race=b'newer competing publication'
  with self.assertRaises(store.PublicationUncertain):self.run_handler()
  self.assertEqual(self.mem.objects[self.mem.head],self.mem.race);self.assertEqual(sum(w['Key']==self.mem.head for w in self.mem.writes),1)
 def test_lost_publication_acknowledgement_never_claims_success_or_rollback(self):
  self.mem.fail=('ack',self.mem.head)
  with self.assertRaises(store.PublicationUncertain):self.run_handler()
  self.safe();self.assertEqual(sum(w['Key']==self.mem.head for w in self.mem.writes),1)
 def test_underflow_and_reported_errors_retained_but_withheld(self):
  self.fill();self.mem.objects['data/short-interest-tickers.json']=b'{"ratio":1e-1000}';self.mem.objects['data/catalyst-calendar.json']=b'{"error":"PRIVATE_CANARY"}'
  self.run_handler();p=self.safe();self.assertEqual(p['inputs']['short_interest']['read_status'],'malformed');self.assertEqual(p['inputs']['catalyst']['read_status'],'reported_error');self.assertNotIn('PRIVATE_CANARY',json.dumps(p));self.assertEqual(p['short_position_context']['by_ticker'],{})
 def test_corrupt_compressed_source_retained_and_other_inputs_still_project(self):
  self.fill();raw=bytearray(gzip.compress(b'{"x":1}',mtime=0));raw[-8]^=255
  self.mem.objects['data/short-interest-tickers.json']=bytes(raw);self.run_handler();p=self.safe()
  si=p['inputs']['short_interest'];self.assertEqual(si['read_status'],'malformed')
  self.assertEqual(self.mem.objects[si['original_ref']['key']],bytes(raw))
  self.assertEqual(p['inputs']['catalyst']['read_status'],'parsed')
  self.assertEqual(p['short_position_context']['by_ticker'],{})
 def test_isolated_surrogate_is_retained_without_breaking_publication_encoding(self):
  self.fill();raw=b'{"contract":"short-interest-tickers.v1","by_ticker":{"TEST":{"dtc_status":"\\ud800"}}}'
  self.mem.objects['data/short-interest-tickers.json']=raw;self.run_handler();p=self.safe()
  si=p['inputs']['short_interest'];self.assertEqual(si['read_status'],'malformed');self.assertEqual(self.mem.objects[si['original_ref']['key']],raw)
if __name__=='__main__':unittest.main(verbosity=2)
