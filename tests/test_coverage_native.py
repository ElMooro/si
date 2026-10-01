"""Whole native/predecessor behavior using complete invented storage inputs."""
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch
from datetime import datetime
import importlib.util,json,sys,unittest
HERE=Path(__file__).resolve().parent
EXTERNAL=(HERE/'coverage_store.py').exists()
ROOT=HERE.parent/'si-batch-improvements' if EXTERNAL else HERE.parent
SOURCE=HERE if EXTERNAL else ROOT/'aws/lambdas/justhodl-coverage-gap-report/source'
sys.path[:0]=[str(SOURCE),str(HERE)]
import coverage_store as store
import coverage_model as model
from test_coverage_store import S3,Error

def native(client):
 spec=importlib.util.spec_from_file_location('coverage_native_candidate',SOURCE/'lambda_function.py');obj=importlib.util.module_from_spec(spec)
 with patch.dict(sys.modules,{'boto3':SimpleNamespace(client=lambda *a,**kw:client),'botocore.config':SimpleNamespace(Config=lambda **kw:kw)}):spec.loader.exec_module(obj)
 return obj

class Tests(unittest.TestCase):
 def test_whole_handler_publishes_scoped_counts_and_no_provider_or_model_calls(self):
  client=S3()
  client.data[model.INPUTS['symbology']]=store.encoded({'n_tickers':999,'by_ticker':{'TEST':{'figi':'BBG000000001','cusip':'123456789'}}})
  client.data[model.INPUTS['edgar']]=store.encoded({'n_filings':2,'complete':False})
  client.data[model.INPUTS['nyfed']]=store.encoded({'rates':{name.lower():{'n_obs':2 if name=='SOFR' else 0} for name in model.RATE_NAMES}})
  client.data[model.INPUTS['rollup']]=store.encoded({'global_feed_counts':{'fred':4}})
  with patch('urllib.request.urlopen',side_effect=AssertionError('No provider requests permitted')):
   result=native(client).lambda_handler({},SimpleNamespace(get_remaining_time_in_millis=lambda:120000))
  self.assertEqual(result['statusCode'],200);self.assertEqual(json.loads(result['body'])['summary'],{'us_tickers':1,'figi_ids':1,'cusips':1,'edgar_filings_qtd':2,'nyfed_reference_rates':1,'fred_feeds_in_use':4})
  packet=json.loads(client.data[store.HEAD]);self.assertFalse(packet['quality']['investment_authority'])
  self.assertEqual(set(packet['replay']['source_files']),{'lambda_function.py','coverage_model.py','coverage_store.py'})
 def test_whole_handler_prior_timeout_preserves_existing_report(self):
  client=S3();raw=store.encoded({'prior':'whole'});client.data[store.HEAD]=raw;client.errors[store.HEAD]=TimeoutError()
  with self.assertRaises(TimeoutError):native(client).lambda_handler({},SimpleNamespace(get_remaining_time_in_millis=lambda:120000))
  self.assertEqual(client.data[store.HEAD],raw);self.assertFalse(client.puts)
 def test_whole_retained_predecessor_reproduces_every_saved_complete_output(self):
  fixture=HERE/'reproduced.json' if EXTERNAL else ROOT/'tests/fixtures/coverage-inventory/reproduced.json'
  predecessor=HERE/'predecessor-lambda.py.txt' if EXTERNAL else ROOT/'tests/fixtures/coverage-inventory/predecessor-lambda.py.txt'
  evidence=json.loads(fixture.read_bytes())
  for name,case in evidence['cases'].items():
   client=S3();client.data={key:json.dumps(value).encode() for key,value in case['inputs'].items()};namespace={'__name__':'whole_coverage_predecessor'}
   with patch.dict(sys.modules,{'boto3':SimpleNamespace(client=lambda *a,**kw:client)}):exec(compile(predecessor.read_bytes(),str(predecessor),'exec'),namespace)
   if case['output']:
    stamp=datetime.fromisoformat(case['output']['as_of']);namespace['datetime']=SimpleNamespace(now=lambda tz:stamp)
   error=None
   try:namespace['lambda_handler']({},None)
   except Exception as exc:error=type(exc).__name__
   self.assertEqual(error,case['error'],name);self.assertEqual(client.reads,case['reads'],name)
   self.assertEqual(json.loads(client.data[store.HEAD]) if store.HEAD in client.data else None,case['output'],name)

if __name__=='__main__':unittest.main()
