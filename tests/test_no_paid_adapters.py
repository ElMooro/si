from pathlib import Path
from unittest.mock import Mock,patch
import contextlib,hashlib,importlib.util,io,json,os,sys,types,unittest,urllib.request
R=Path(__file__).resolve().parents[1];D=R/'tests/fixtures/no-paid-adapters'

def load(name,old=False):
 path=D/(name+'.py.before.txt') if old else R/'aws/shared'/(name+'.py')
 module=types.ModuleType('invented_'+name);module.__file__=str(path)
 exec(compile(path.read_bytes(),str(path),'exec'),module.__dict__)
 return module

class NoPaidAdapters(unittest.TestCase):
 def setUp(self):
  self.stack=contextlib.ExitStack();self.addCleanup(self.stack.close)
  self.transport=self.stack.enter_context(patch.object(urllib.request,'urlopen',Mock(side_effect=AssertionError('Unexpected transport'))))
  self.secret=Mock(side_effect=AssertionError('Unexpected credential service'))
  self.stack.enter_context(patch.dict(sys.modules,{'boto3':types.SimpleNamespace(client=self.secret),'llm_cost':None}))
  self.stack.enter_context(patch.dict(os.environ,{'ANTHROPIC_KEY':'INVENTED_CANARY','ANTHROPIC_API_KEY':'INVENTED_CANARY','XAI_API_KEY':'INVENTED_CANARY','ZAI_API_KEY':'INVENTED_CANARY'},clear=True))
  self.log=io.StringIO();self.stack.enter_context(contextlib.redirect_stdout(self.log))
 def assert_no_requests(self):
  self.transport.assert_not_called();self.secret.assert_not_called();self.assertEqual(self.log.getvalue(),'')
 def test_complete_all_tiers_never_uses_missing_cost_service_or_provider(self):
  router=load('llm_router');router._claude=Mock(side_effect=AssertionError('Provider'));router._glm=Mock(side_effect=AssertionError('Provider'))
  for tier in ('bulk','reason','critical','grok','unknown'):
   for private in (False,True):self.assertEqual(router.complete('invented prompt',tier=tier,contains_proprietary=private,no_cache=True,on_demand=True),'')
  router._claude.assert_not_called();router._glm.assert_not_called();self.assert_no_requests()
 def test_complete_never_reads_cached_model_text_or_budget_policy(self):
  forbidden=Mock(side_effect=AssertionError('Unexpected cache or policy request'))
  costs=types.SimpleNamespace(economy_downgrade=forbidden,cache_get=forbidden,budget_ok=forbidden)
  with patch.dict(sys.modules,{'llm_cost':costs}):self.assertEqual(load('llm_router').complete('invented'),'')
  forbidden.assert_not_called();self.assert_no_requests()
 def test_prompt_and_system_are_not_stringified_or_logged(self):
  class Untouched:
   def __str__(self):raise AssertionError('Unexpected prompt conversion')
  router=load('llm_router');self.assertEqual(router.complete(Untouched(),system=Untouched()),'')
  self.assertTrue(all(not v['ok'] for v in router.council(Untouched(),system=Untouched(),context=Untouched()).values()));self.assert_no_requests()
 def test_direct_provider_helpers_and_key_helpers_cannot_bypass_policy(self):
  router=load('llm_router')
  self.assertEqual(router._ANTHROPIC_KEY,'')
  for name in ('_anthropic_key','_zai_key','_pplx_key'):self.assertEqual(getattr(router,name)(),'')
  self.assertEqual(router._claude('invented','ignored',100),('',0,0))
  self.assertEqual(router._glm('invented','ignored',100),('',0,0));self.assertEqual(router._glm('invented','ignored',100,kind='xai'),('',0,0))
  self.assertEqual(router._perplexity('invented'),('',[]));self.assert_no_requests()
 def test_council_has_explicit_absence_and_no_provider_answers_or_citations(self):
  router=load('llm_router');out=router.council('INVENTED_PROMPT_CANARY',providers=['claude','glm','perplexity','unknown'])
  self.assertEqual(set(out),{'claude','glm','perplexity','unknown'})
  for item in out.values():
   self.assertIs(item['ok'],False);self.assertIs(item['attempted'],False);self.assertEqual(item['answer'],'');self.assertEqual(item['citations'],[]);self.assertEqual(item['error_class'],'policy_blocked');self.assertIsNone(item['latency_s'])
  self.assertNotIn('INVENTED_PROMPT_CANARY',repr(out));self.assert_no_requests()
 def test_xai_voice_cannot_read_env_key_or_fallback_to_ssm(self):
  voice=load('xai_voice');self.assertEqual(voice._key_get(),'');self.assertEqual(voice.complete('invented'),'');self.assert_no_requests()
 def test_claude_compat_never_falls_back_to_direct_anthropic(self):
  compat=load('claude_compat');body={'messages':[{'content':'INVENTED_PROMPT_CANARY'}]}
  out=compat.messages(body);self.assertEqual(out['content'],[{'type':'text','text':''}]);self.assertFalse(out['_model_request_attempted']);self.assertEqual(out['_status'],'policy_blocked');self.assertEqual(compat.text(body),'');self.assertNotIn('INVENTED_PROMPT_CANARY',repr(out));self.assert_no_requests()
 def shim(self):
  flag='_anthropic_shim_patched';exists=hasattr(urllib.request,flag);old=getattr(urllib.request,flag,None)
  if exists:delattr(urllib.request,flag)
  def restore():
   if exists:setattr(urllib.request,flag,old)
   elif hasattr(urllib.request,flag):delattr(urllib.request,flag)
  self.addCleanup(restore)
  return load('anthropic_shim')
 def test_urllib_internal_header_and_invalid_body_cannot_bypass_host_denial(self):
  self.shim()
  for domain in ('api.anthropic.com','api.z.ai','api.x.ai','api.perplexity.ai','api.openai.com'):
   req=urllib.request.Request('https://'+domain+'/invented',data=b'not json',headers={'x-jh-internal':'router'})
   with urllib.request.urlopen(req) as resp:out=json.loads(resp.read())
   self.assertEqual(out['_status'],'policy_blocked');self.assertFalse(out['_model_request_attempted']);self.assertEqual(out['content'][0]['text'],'')
  self.assert_no_requests()
 def test_string_urls_and_mixed_case_hosts_are_also_blocked(self):
  self.shim()
  out=json.loads(urllib.request.urlopen('https://API.ANTHROPIC.COM/v1/messages',data=b'{}').read())
  self.assertEqual(out['_status'],'policy_blocked');self.assert_no_requests()
 def test_unrelated_data_requests_are_not_silently_treated_as_models(self):
  self.shim();sentinel=object();self.transport.side_effect=None;self.transport.return_value=sentinel
  for url in ('https://example.invalid/data','https://example.invalid/?next=api.anthropic.com','https://api.anthropic.com.example.invalid/data'):
   self.assertIs(urllib.request.urlopen(url),sentinel)
  self.assertEqual(self.transport.call_count,3);self.secret.assert_not_called()
 def test_shim_reinstallation_does_not_stack_or_restore_paid_routing(self):
  shim=self.shim();installed=urllib.request.urlopen;shim._install();self.assertIs(urllib.request.urlopen,installed)
  self.assertEqual(json.loads(urllib.request.urlopen('https://api.anthropic.com').read())['_status'],'policy_blocked');self.assert_no_requests()
 def test_full_predecessors_and_exact_edits_reconstruct_all_candidate_sources(self):
  plans=json.loads((D/'adapter-edits.json').read_bytes())
  for path,item in plans.items():
   name=Path(path).name;raw=(D/(name+'.before.txt')).read_bytes();self.assertEqual(hashlib.sha256(raw).hexdigest(),item['sha256']);text=raw.decode()
   for before,after in item['edits']:self.assertEqual(text.count(before),1);text=text.replace(before,after)
   self.assertEqual(text,(R/path).read_text(encoding='utf-8'));self.assertEqual(hashlib.sha256(text.encode()).hexdigest(),item['candidate_sha256'])
 def test_predecessor_cost_import_failure_really_reached_provider(self):
  router=load('llm_router',old=True);router._claude=Mock(return_value=('invented answer',1,1))
  self.assertEqual(router.complete('invented'),'invented answer');router._claude.assert_called_once();self.assert_no_requests()
 def test_predecessor_council_bypassed_paid_budget_admission(self):
  router=load('llm_router',old=True);router._claude=Mock(return_value=('invented answer',1,1))
  self.assertTrue(router.council('invented',providers=['claude'])['claude']['ok']);router._claude.assert_called_once();self.assert_no_requests()

if __name__=='__main__':unittest.main(verbosity=2)
