from pathlib import Path
from unittest.mock import Mock,patch
import ast,copy,hashlib,io,json,runpy,sys,types,unittest
R=Path(__file__).resolve().parents[1];W=R;sys.path.insert(0,str(R/'aws/shared'))
import crypto_sentiment_observations as model
import crypto_sentiment_archive as archive
RAW=b'{"name":"Fear and Greed Index","data":[{"value":"0","timestamp":"1577836800"}],"metadata":{"error":null}}'
def function(engine,name,scope):
 path=R/'aws/lambdas'/engine/'source/lambda_function.py';tree=ast.parse(path.read_bytes());node=next(n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name==name)
 node.decorator_list=[];exec(compile(ast.Module(body=[node],type_ignores=[]),str(path),'exec'),scope);return scope[name]
def retained():
 class Response(io.BytesIO):
  status=200;headers={}
  def __init__(self,raw,url):super().__init__(raw);self.url=url
  def geturl(self):return self.url
 saved={};client=Mock();client.put_object.side_effect=lambda **kw:saved.update({kw['Key']:kw['Body']});client.get_object.side_effect=lambda **kw:{'Body':io.BytesIO(saved[kw['Key']]),'ContentLength':len(saved[kw['Key']])}
 opener=Mock(side_effect=lambda req,timeout:Response(RAW if 'alternative' in req.full_url else b'{"prices":[]}',req.full_url))
 collector=function('justhodl-crypto-intel','fetch_fg',{'urllib':types.SimpleNamespace(request=types.SimpleNamespace(urlopen=opener)),'S3_BUCKET':'invented'})
 with patch.object(archive,'storage_client',return_value=client):p=collector(types.SimpleNamespace(get_remaining_time_in_millis=lambda:180000))
 return p,client,opener,saved
class Consumers(unittest.TestCase):
 def test_actual_producer_captures_and_replays_existing_requests_then_publishes(self):
  p,client,opener,saved=retained();self.assertEqual(opener.call_count,3);self.assertEqual(len(saved),5);self.assertEqual(model.context(p)['current'],0);self.assertIsNone(p['avg_7d']);client.close.assert_called_once()
 def test_actual_producer_does_not_publish_after_archive_failure(self):
  fn=function('justhodl-crypto-intel','fetch_fg',{'urllib':types.SimpleNamespace(request=types.SimpleNamespace(urlopen=Mock())),'S3_BUCKET':'invented'})
  with patch.object(archive,'collect_retained',side_effect=ValueError('invented archive denial')):
   with self.assertRaises(ValueError):fn()
 def test_executor_supplies_real_remaining_time_context(self):
  tree=ast.parse((R/'aws/lambdas/justhodl-crypto-intel/source/lambda_function.py').read_bytes());calls=[n for n in ast.walk(tree) if isinstance(n,ast.Call) and isinstance(n.func,ast.Attribute) and n.func.attr=='submit' and n.args and isinstance(n.args[0],ast.Name) and n.args[0].id=='fetch_fg'];self.assertEqual(len(calls),1);self.assertEqual([n.id for n in calls[0].args],['fetch_fg','context'])
 def test_sentiment_cannot_change_risk_votes_or_trigger_null_comparison_crash(self):
  sys.path.insert(0,str(R/'aws/shared'))
  fn=function('justhodl-crypto-intel','risk',{});baseline=fn({}, {}, {}, {}, {})
  for p in [None,{},True,{'current':True},{'current':0},{'current':100},retained()[0]]:
   result=fn(p,{},{},{},{});self.assertEqual(result,baseline);self.assertIsNone(result['score']);self.assertEqual(result['action'],'WAIT')
 def test_financial_context_replays_whole_original_and_withholds_forged_or_legacy_current(self):
  good=retained()[0];bad=copy.deepcopy(good);bad['current']=99
  for p,expected in [(good,0),(bad,None),({'current':0},None),({'value':99},None),({},None)]:
   def get(**kw):return {'Body':io.BytesIO(json.dumps({'fear_greed':p} if kw['Key']=='crypto-intel.json' else {}).encode())}
   out=function('justhodl-financial-secretary','fetch_tier2',{'s3':types.SimpleNamespace(get_object=get),'json':json,'BUCKET':'invented'})()['crypto']
   self.assertEqual(out['fear_greed_value'],expected);self.assertFalse(out['fear_greed_research']['calls_eligible'])
 def test_telegram_zero_preserved_without_sending_or_using_legacy_report_value(self):
  good=retained()[0]
  for p,expected in [(good,0),({'current':99},None),(True,None)]:
   out=function('justhodl-telegram-bot','enrich_with_crypto_intel',{'get_crypto_intel':lambda:{'fear_greed':p}})({'fear_greed':70})
   self.assertEqual(out['fear_greed'],expected);self.assertFalse(out['fear_greed_research']['forecast_qualified'])
 def test_allocator_never_awards_descriptive_sentiment_points(self):
  for p in [retained()[0],{'value':0},{'value':100},True]:
   add=Mock(side_effect=AssertionError('unregistered vote'));score={'BTC':3};evidence=['other'];fn=function('justhodl-allocator','rule_btc_signals',{'fs3':lambda key:{'fear_greed':p},'add':add});fn(score,evidence);self.assertEqual(score,{'BTC':3});self.assertEqual(evidence,['other']);add.assert_not_called()
 def test_signal_logger_sentiment_segment_cannot_write_outcomes(self):
  text=(R/'aws/lambdas/justhodl-signal-logger/source/lambda_function.py').read_text(encoding='utf-8');tree=ast.parse(text);handler=next(n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name=='lambda_handler');start=next(i for i,n in enumerate(handler.body) if isinstance(n,ast.Assign) and any(isinstance(t,ast.Name) and t.id=='c' for t in n.targets));end=next(i for i,n in enumerate(handler.body[start:],start) if isinstance(n,ast.Assign) and any(isinstance(t,ast.Name) and t.id=='rs' for t in n.targets));segment=ast.Module(body=handler.body[start:end],type_ignores=[])
  for p in [retained()[0],{'current':0},{'current':100}]:
   write=Mock(side_effect=AssertionError('outcome write'));scope={'fs3':lambda key:{'fear_greed':p},'logged':[],'log_sig':write};exec(compile(segment,'actual_sentiment_segment','exec'),scope);self.assertEqual(scope['logged'],[]);write.assert_not_called()
 def test_other_provider_failure_does_not_trigger_additional_price_request(self):
  import crypto_sentiment_transport as t
  op=Mock(side_effect=TimeoutError());p=t.collect(op,model,lambda:180000);self.assertEqual(op.call_count,2);self.assertEqual(p['source_attempts'][2]['transport_error'],'upstream_prerequisite_unavailable');self.assertIsNone(p['current']);self.assertEqual(p['status'],'unavailable')
 def test_absent_telegram_source_cannot_reuse_report_sentiment(self):
  for source in [None,{},True,[],False]:
   out=function('justhodl-telegram-bot','enrich_with_crypto_intel',{'get_crypto_intel':lambda:source})({'fear_greed':99})
   self.assertIsNone(out['fear_greed']);self.assertIsNone(out['fear_greed_research']['source_observation_at'])
 def actual_context_statements(self,engine,target):
  tree=ast.parse((R/'aws/lambdas'/engine/'source/lambda_function.py').read_bytes())
  for parent in ast.walk(tree):
   for _,body in ast.iter_fields(parent):
    if not isinstance(body,list):continue
    for i,node in enumerate(body):
     if isinstance(node,ast.Assign) and any(isinstance(t,ast.Name) and t.id==target for t in node.targets) and 'crypto_sentiment_observations' in ast.dump(node):return body[i:i+2],tree
  raise AssertionError('Actual consumer context assignment missing')
 def test_chat_context_exposes_unit_dates_source_and_denied_authority(self):
  nodes,_=self.actual_context_statements('justhodl-ai-chat','fg')
  for p,expected in [(retained()[0],0),({'value':99},None)]:
   scope={'cd':{'fear_greed':p},'json':json,'lines':[]};exec(compile(ast.Module(body=nodes,type_ignores=[]),'actual_chat_context','exec'),scope)
   self.assertEqual(scope['fg']['current'],expected);self.assertEqual(len(scope['lines']),1);self.assertIn('Alternative.me',scope['lines'][0]);self.assertIn('source_observation_at',scope['lines'][0]);self.assertIn('index_points',scope['lines'][0]);self.assertFalse(scope['fg']['calls_eligible'])
 def test_cycle_factor_never_maps_sentiment_to_risk_or_weight(self):
  nodes,_=self.actual_context_statements('justhodl-crypto-cycle-risk','sentiment')
  for p,expected in [(retained()[0],0),({'current':100},None)]:
   scope={'crypto':{'fear_greed':p},'factors':{}};exec(compile(ast.Module(body=nodes,type_ignores=[]),'actual_cycle_context','exec'),scope);factor=scope['factors']['fear_greed'];self.assertEqual(factor['weight'],0);self.assertIsNone(factor['risk']);self.assertEqual(factor['value'],expected);self.assertFalse(factor['sizing_eligible'])
 def test_morning_summary_uses_verified_projection_and_retains_full_context(self):
  nodes,tree=self.actual_context_statements('justhodl-morning-intelligence','fg');projection=next(n for n in ast.walk(tree) if isinstance(n,ast.Dict) and any(isinstance(k,ast.Constant) and k.value=='fg_research' for k in n.keys));pairs=[(k,v) for k,v in zip(projection.keys,projection.values) if isinstance(k,ast.Constant) and k.value in ('fg','fg_label','fg_research')]
  for p,expected in [(retained()[0],0),({'current':100},None)]:
   scope={'crypto':{'fear_greed':p}};exec(compile(ast.Module(body=nodes[:1],type_ignores=[]),'actual_morning_context','exec'),scope);out=eval(compile(ast.fix_missing_locations(ast.Expression(ast.Dict(keys=[k for k,v in pairs],values=[v for k,v in pairs]))),'actual_morning_projection','eval'),scope);self.assertEqual(out['fg'],expected);self.assertFalse(out['fg_research']['calls_eligible']);self.assertEqual(out['fg_research'],scope['fg'])
 def test_every_reviewed_primary_edit_preserves_complete_predecessor_bytes(self):
  fixture=R/'tests/fixtures/crypto-sentiment';plans=json.loads((fixture/'edits.json').read_bytes())
  for path,plan in plans.items():
   raw=(fixture/'before'/(path+'.txt')).read_bytes();self.assertEqual(hashlib.sha256(raw).hexdigest(),plan['predecessor_sha256']);text=raw.decode('utf-8')
   for old,new in plan['edits']:self.assertEqual(text.count(old),1,path);text=text.replace(old,new)
   self.assertEqual(text.encode(),(R/path).read_bytes(),path);self.assertEqual(hashlib.sha256(text.encode()).hexdigest(),plan['candidate_sha256'])
 def test_financial_html_uses_checked_context_instead_of_legacy_scalar(self):
  support=runpy.run_path(str(R/'aws/lambdas/justhodl-financial-secretary/tests/run_tests.py'))
  good=model.context(retained()[0])
  for context,expected in [(good,'0/100'),({},'Unavailable'),(True,'Unavailable'),({**good,'calls_eligible':True},'Unavailable'),({**good,'current':False},'Unavailable')]:
   payload=support['scan']();payload['tier2']['crypto'].update(fear_greed_research=context,fear_greed_value=99)
   html=support['SCOPE']['build_email_html'](payload)
   self.assertIn('BITCOIN SENTIMENT</div><div style="font-size:22px;font-weight:700">'+expected,html);self.assertIn('Alternative.me',html);self.assertIn('descriptive only, no forecast or sizing vote',html)
if __name__=='__main__':unittest.main(verbosity=2)
