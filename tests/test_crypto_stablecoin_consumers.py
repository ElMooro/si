"""Actual stablecoin producer/consumer regressions using invented I/O only."""
from pathlib import Path
from unittest.mock import Mock,patch
import ast,copy,gzip,hashlib,io,json,runpy,sys,types,unittest
R=Path(__file__).resolve().parents[1];sys.path.insert(0,str(R/'aws/shared'))
import crypto_stablecoin_observations as model
import crypto_stablecoin_archive as archive
RAW=b'{"peggedAssets":[{"id":"invented","pegType":"peggedUSD","circulating":{"peggedUSD":0}}]}'
def function(engine,name,scope):
 path=R/'aws/lambdas'/engine/'source/lambda_function.py';tree=ast.parse(path.read_bytes())
 node=next(n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name==name)
 exec(compile(ast.Module(body=[node],type_ignores=[]),str(path),'exec'),scope);return scope[name]
def retained():
 class Response(io.BytesIO):
  status=200;headers={}
  def geturl(self):return 'https://stablecoins.llama.fi/stablecoins?includePrices=true'
 client=Mock();saved={}
 client.put_object.side_effect=lambda **kw:saved.update({kw['Key']:kw['Body']})
 client.get_object.side_effect=lambda **kw:{'Body':io.BytesIO(saved[kw['Key']]),'ContentLength':len(saved[kw['Key']])}
 opener=Mock(return_value=Response(RAW))
 collector=function('justhodl-crypto-intel','fetch_stablecoins',{'urllib':types.SimpleNamespace(request=types.SimpleNamespace(urlopen=opener)),'S3_BUCKET':'invented'})
 with patch.object(archive,'storage_client',return_value=client):packet=collector(types.SimpleNamespace(get_remaining_time_in_millis=lambda:180000))
 return packet,client,opener,saved
class Consumers(unittest.TestCase):
 def test_financial_functions_and_unrelated_code_remain_complete(self):
  target='aws/lambdas/justhodl-financial-secretary/source/lambda_function.py'
  before=ast.parse((R/'tests/fixtures/crypto-stablecoin-stocks/before'/(target+'.txt')).read_bytes());after=ast.parse((R/target).read_bytes())
  def untouched(tree):return ast.dump(ast.Module(body=[n for n in tree.body if not isinstance(n,ast.FunctionDef) or n.name not in {'fetch_tier2','build_email_html'}],type_ignores=[]))
  self.assertEqual(untouched(before),untouched(after))
  self.assertEqual([n.name for n in before.body if isinstance(n,ast.FunctionDef)],[n.name for n in after.body if isinstance(n,ast.FunctionDef)])
 def test_actual_producer_retains_original_then_replays_without_extra_provider_request(self):
  packet,client,opener,saved=retained();self.assertEqual(opener.call_count,1);self.assertEqual(len(saved),5)
  self.assertEqual(packet['stablecoins'][0]['snapshots']['current']['value'],0);self.assertTrue(packet['original_capture']['complete_capture_replayed'])
  self.assertEqual(model.context(packet)['status'],'descriptive');client.close.assert_called_once()
 def test_actual_producer_will_not_publish_unretained_values(self):
  opener=Mock();fn=function('justhodl-crypto-intel','fetch_stablecoins',{'urllib':types.SimpleNamespace(request=types.SimpleNamespace(urlopen=opener)),'S3_BUCKET':'invented'})
  with patch.object(archive,'collect_retained',side_effect=ValueError('invented storage failure')):
   with self.assertRaises(ValueError):fn(types.SimpleNamespace(get_remaining_time_in_millis=lambda:180000))
  opener.assert_not_called()
 def test_real_executor_passes_remaining_time_context_to_stock_collector(self):
  tree=ast.parse((R/'aws/lambdas/justhodl-crypto-intel/source/lambda_function.py').read_bytes())
  calls=[n for n in ast.walk(tree) if isinstance(n,ast.Call) and isinstance(n.func,ast.Attribute) and n.func.attr=='submit' and n.args and isinstance(n.args[0],ast.Name) and n.args[0].id=='fetch_stablecoins']
  self.assertEqual(len(calls),1);self.assertEqual([n.id for n in calls[0].args],['fetch_stablecoins','context'])
 def test_legacy_flows_never_change_even_unqualified_risk_or_allow_trades(self):
  fn=function('justhodl-crypto-intel','risk',{});baseline=fn({}, {}, {}, {}, {})
  for packet in [{'net_signal':'INFLOW'},{'net_signal':'OUTFLOW'},{'net_signal':'NEUTRAL'},retained()[0]]:
   result=fn({}, {}, packet, {}, {});self.assertEqual(result,baseline);self.assertEqual(result['action'],'WAIT');self.assertIsNone(result['score']);self.assertFalse(result['calls_eligible'])
 def test_financial_projection_checks_whole_stock_original_and_never_passes_legacy_flow(self):
  valid=retained()[0];changed=copy.deepcopy(valid);changed['stablecoins'][0]['snapshots']['current']['value']=999
  for stock,expected in [(valid,'descriptive'),(changed,'unavailable'),({'net_signal':'INFLOW','minting_count':9,'burning_count':0},'unavailable'),({},'unavailable')]:
   def get(**kw):return {'Body':io.BytesIO(json.dumps({'stablecoins':stock,'fear_greed':{'current':0},'generated_at':'2040-01-01T00:00:00Z'} if kw['Key']=='crypto-intel.json' else {}).encode())}
   result=function('justhodl-financial-secretary','fetch_tier2',{'s3':types.SimpleNamespace(get_object=get),'json':json,'BUCKET':'invented'})()['crypto']
   for key in ('stablecoin_net_signal','stablecoin_minting','stablecoin_burning'):self.assertIsNone(result[key])
   self.assertEqual(result['stablecoin_research']['status'],expected);self.assertIsNone(result['fear_greed_value']);self.assertEqual(result['timestamp'],'2040-01-01T00:00:00Z')
 def test_every_reviewed_edit_preserves_complete_predecessor_bytes(self):
  fixture=R/'tests/fixtures/crypto-stablecoin-stocks';plans=json.loads(gzip.decompress((fixture/'edits.json.gz').read_bytes()))
  for path,plan in plans.items():
   raw=(fixture/'before'/(path+'.txt')).read_bytes();self.assertEqual(hashlib.sha256(raw).hexdigest(),plan['predecessor_sha256']);text=raw.decode('utf-8')
   for old,new in plan['edits']:self.assertEqual(text.count(old),1,path);text=text.replace(old,new)
   self.assertEqual(text.encode(),(R/path).read_bytes(),path);self.assertEqual(hashlib.sha256(text.encode()).hexdigest(),plan['candidate_sha256'])
 def test_financial_html_keeps_stock_count_separate_from_flow_and_handles_legacy(self):
  support=runpy.run_path(str(R/'aws/lambdas/justhodl-financial-secretary/tests/run_tests.py'))
  valid=model.context(retained()[0])
  for packet,expected in [(valid,'1 reported rows'),({},'Unavailable'),('malformed','Unavailable'),([1],'Unavailable'),(True,'Unavailable'),({**valid,'independent_investment_votes':1},'Unavailable'),({**valid,'independent_investment_votes':False},'Unavailable')]:
   payload=support['scan']();payload['tier2']['crypto'].update(stablecoin_research=packet,stablecoin_net_signal='INFLOW')
   html=support['SCOPE']['build_email_html'](payload)
   self.assertIn('REPORTED STABLECOIN STOCKS',html);self.assertIn(expected,html);self.assertIn('Flow and observation dates unavailable',html);self.assertNotIn('STABLECOIN FLOW',html);self.assertNotIn('INFLOW',html)
if __name__=='__main__':unittest.main(verbosity=2)
