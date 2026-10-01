from pathlib import Path
from unittest.mock import Mock,patch
from datetime import datetime,timezone
import ast,contextlib,hashlib,io,json,sys,types,unittest,urllib.request
R=Path(__file__).resolve().parents[1];D=R/'tests/fixtures/no-paid-adapters'

def method(engine,name,extra=None,old=False):
 path=D/(engine+'.before.py.txt') if old else R/'aws/lambdas'/engine/'source/lambda_function.py';tree=ast.parse(path.read_text(encoding='utf-8'));node=next(n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name==name)
 env={'__file__':str(path),'datetime':datetime,'timezone':timezone,'json':json,'urllib':types.SimpleNamespace(request=types.SimpleNamespace(Request=urllib.request.Request,urlopen=Mock(side_effect=AssertionError('Unexpected transport')))),**(extra or {})}
 exec(compile(ast.Module(body=[node],type_ignores=[]),str(path),'exec'),env)
 return env[name],env

class CallerBoundaries(unittest.TestCase):
 def test_brain_notes_never_reach_fallback_transport_when_router_is_empty(self):
  call,scope=method('justhodl-brain-sync','_llm',{'ANTHROPIC_KEY':'INVENTED','MODEL':'invented'})
  router=types.SimpleNamespace(complete=Mock(return_value=''))
  with patch.dict(sys.modules,{'llm_router':router}):self.assertIsNone(call('invented notes','invented system',100))
  router.complete.assert_not_called();scope['urllib'].request.urlopen.assert_not_called()
 def test_predecessor_brain_fallback_bypassed_empty_router(self):
  call,scope=method('justhodl-brain-sync','_llm',{'ANTHROPIC_KEY':'INVENTED','MODEL':'invented'},old=True)
  response=Mock();response.read.return_value=b'{"content":[{"type":"text","text":"invented"}]}'
  scope['urllib'].request.urlopen.side_effect=None;scope['urllib'].request.urlopen.return_value=response
  with patch.dict(sys.modules,{'llm_router':types.SimpleNamespace(complete=Mock(return_value=''))}):self.assertEqual(call('invented notes','invented system',100),'invented')
  scope['urllib'].request.urlopen.assert_called_once()
 def test_glm_absence_is_not_labeled_success_or_metered_as_a_model_call(self):
  call,scope=method('justhodl-ai','_glm_complete');provider=Mock(side_effect=AssertionError('Unexpected provider'));record=Mock(side_effect=AssertionError('Unexpected meter'))
  with patch.dict(sys.modules,{'llm_router':types.SimpleNamespace(_glm=provider),'llm_cost':types.SimpleNamespace(record=record)}):self.assertEqual(call('invented'),'')
  self.assertIn('disabled',call.last_path);self.assertNotIn(' ok ',call.last_path);provider.assert_not_called();record.assert_not_called()
 def test_predecessor_glm_marked_empty_result_ok(self):
  call,scope=method('justhodl-ai','_glm_complete',old=True)
  with patch.dict(sys.modules,{'llm_router':types.SimpleNamespace(_glm=Mock(return_value=('',0,0))),'llm_cost':None}):self.assertEqual(call('invented'),'')
  self.assertIn(' ok ',call.last_path)
 def test_crypto_commentary_has_no_forecast_permission_or_source_reads(self):
  read=Mock(side_effect=AssertionError('Unexpected source read'));post=Mock(side_effect=AssertionError('Unexpected provider'))
  call,scope=method('justhodl-crypto-intel','gen_ai',{'s3_read':read,'http_post':post});out=call({},{});
  self.assertEqual(out['status'],'unavailable');self.assertIsNone(out['analysis']);self.assertIsNone(out['model']);self.assertFalse(out['model_request_attempted'])
  for key in ('calls_eligible','sizing_eligible','execution_eligible','forecast_qualified'):self.assertIs(out[key],False)
  self.assertEqual(out['independent_investment_votes'],0);self.assertTrue(all(v is None for v in out['macro'].values()));read.assert_not_called();post.assert_not_called()
 def test_crypto_does_not_inspect_or_discard_given_measurement_objects(self):
  class Unread:
   def get(self,*args):raise AssertionError('Model helper must not inspect measurements')
  call,_=method('justhodl-crypto-intel','gen_ai');self.assertEqual(call(Unread(),Unread())['reason'],'paid_model_api_disabled')
 def test_predecessor_crypto_labeled_empty_answer_ok(self):
  call,scope=method('justhodl-crypto-intel','gen_ai',{'ANTHROPIC_API_KEY':'INVENTED','s3_read':Mock(return_value={})},old=True)
  with patch.dict(sys.modules,{'llm_router':types.SimpleNamespace(complete=Mock(return_value=''))}),contextlib.redirect_stdout(io.StringIO()):out=call({}, {})
  self.assertEqual(out['status'],'ok');self.assertEqual(out['analysis'],'');self.assertEqual(out['model'],'glm-5.1')
 def test_legacy_flow_helper_blocks_even_without_router_or_credentials(self):
  call,scope=method('justhodl-flows-ai-analysis','claude_call')
  with patch.dict(sys.modules,{'llm_router':None}),self.assertRaisesRegex(RuntimeError,'^paid_model_api_disabled$'):call('invented')
  scope['urllib'].request.urlopen.assert_not_called()
 def test_strategist_actual_reasoning_block_is_explicit_absence(self):
  text=(R/'aws/lambdas/justhodl-strategist/source/lambda_function.py').read_text(encoding='utf-8')
  handler=next(n for n in ast.parse(text).body if isinstance(n,ast.FunctionDef) and n.name=='lambda_handler')
  selected=[n for n in handler.body if isinstance(n,ast.Assign) and any(isinstance(t,ast.Name) and t.id in ('model_used','reasoning') for t in n.targets)]
  self.assertEqual(len(selected),2);scope={};exec(compile(ast.Module(body=selected,type_ignores=[]),'actual_reasoning','exec'),scope)
  self.assertIsNone(scope['model_used']);self.assertEqual(scope['reasoning']['status'],'unavailable');self.assertFalse(scope['reasoning']['model_request_attempted'])
  self.assertFalse(any(isinstance(n,ast.ImportFrom) and n.module=='llm_router' for n in ast.walk(handler)))
  payload=next(n.value for n in handler.body if isinstance(n,ast.Assign) and any(isinstance(t,ast.Name) and t.id=='payload' for t in n.targets))
  fields={k.value:v for k,v in zip(payload.keys,payload.values) if isinstance(k,ast.Constant)}
  for name in ('calls_eligible','sizing_eligible','execution_eligible','forecast_qualified'):self.assertIs(ast.literal_eval(fields[name]),False)
  self.assertEqual(ast.literal_eval(fields['independent_investment_votes']),0)
  self.assertNotIn('quality roughly doubles',text)
 def test_existing_regressions_keep_all_assertions_except_exact_policy_expectations(self):
  for name,archive in [('test-fixture-edit.json','offexchange-consumer-tests.before.txt'),('ai-runner-edit.json','ai-runner.before.txt')]:
   edit=json.loads((D/name).read_bytes());raw=(D/archive).read_bytes();self.assertEqual(hashlib.sha256(raw).hexdigest(),edit['predecessor_sha256']);text=raw.decode()
   for a,b in edit['edits']:self.assertEqual(text.count(a),1);text=text.replace(a,b)
   self.assertEqual(text,(R/edit['target']).read_text(encoding='utf-8'))
 def test_full_source_predecessors_and_exact_helper_deltas_are_preserved(self):
  for path,p in json.loads((D/'caller-edits.json').read_bytes()).items():
   raw=(D/(p['function']+'.before.py.txt')).read_bytes();self.assertEqual(hashlib.sha256(raw).hexdigest(),p['predecessor_sha256']);text=raw.decode('utf-8')
   for a,b in p['edits']:self.assertEqual(text.count(a),1);text=text.replace(a,b)
   self.assertEqual(text,(R/path).read_text(encoding='utf-8'));self.assertEqual(hashlib.sha256(text.encode()).hexdigest(),p['candidate_sha256'])

if __name__=='__main__':unittest.main(verbosity=2)
