"""Whole Options Confluence handlers with invented diagnostic coverage only."""
from pathlib import Path
from datetime import datetime,timezone
from unittest.mock import patch
import contextlib,io,json,sys,types,unittest
R=Path(__file__).resolve().parents[1];sys.path.insert(0,str(R/'aws/shared'))
import ticker_coverage_context as helper
class Clock(datetime):
 @classmethod
 def now(cls,tz=None):return datetime(2020,1,1,tzinfo=timezone.utc)
def run(value,old=False):
 writes=[];streams=[]
 def get(**kw):
  key=kw['Key']
  packet=value if key==helper.SOURCE else {'setups':[{'ticker':'TEST'}]} if key=='data/squeeze-pretrigger.json' else {}
  if isinstance(packet,Exception):raise packet
  raw=packet if isinstance(packet,bytes) else json.dumps(packet).encode();stream=io.BytesIO(raw);streams.append(stream)
  return {'Body':stream,'ContentLength':len(raw)}
 client=types.SimpleNamespace(get_object=get,put_object=lambda **kw:writes.append(json.loads(kw['Body'])))
 path=R/'aws/lambdas/justhodl-options-confluence/source/lambda_function.py'
 if old:path=R/'tests/fixtures/crypto-funding-archive/before/aws/lambdas/justhodl-options-confluence/source/lambda_function.py.txt'
 scope={}
 with patch.dict(sys.modules,{'boto3':types.SimpleNamespace(client=lambda *a,**kw:client),'ticker_coverage_context':helper}),patch('urllib.request.urlopen',side_effect=AssertionError('No network')) as transport,contextlib.redirect_stdout(io.StringIO()):
  exec(compile(path.read_bytes(),str(path),'exec'),scope);scope.update(datetime=Clock,time=types.SimpleNamespace(time=lambda:100))
  scope['lambda_handler']({},None);transport.assert_not_called()
 assert len(writes)==1
 return writes[0]
def packet(count=0):return {'contract':'ticker-360.v1','tickers':{'TEST':{'coverage_count':count,'domains':{}}}}
class Publication(unittest.TestCase):
 def test_whole_output_matches_peer_predecessor_except_retained_diagnostic_evidence(self):
  for value in [packet(),{},packet(True),{'contract':'ticker-360.v1','tickers':{'TEST':[1]}}]:
   result=run(value);context=result.pop('ticker_360_context');self.assertEqual(result,run(value,True));self.assertEqual(context['additional_independent_votes'],0)
 def test_missing_is_not_zero_and_real_zero_stays_zero(self):
  for value,expected in [({},None),(packet(),0)]:
   result=run(value);row=result['by_posture']['COILED'][0];self.assertEqual(row['t360_coverage'],expected)
   self.assertEqual(result['counts']['names'],1);self.assertEqual(row['n_engines'],1)
 def test_denial_and_invalid_json_are_visible_without_error_leak_or_votes(self):
  for value in [PermissionError('PRIVATE_CANARY'),b'{"x":NaN}',b'{"tickers":{},"tickers":{}}']:
   result=run(value);context=result['ticker_360_context'];self.assertEqual(context['status'],'unavailable');self.assertNotIn('PRIVATE_CANARY',json.dumps(result));self.assertTrue(all(context[key] is False for key in helper.FLAGS))
 def test_unreconciled_coverage_retains_reason_without_acquiring_independence(self):
  result=run(packet(3));context=result['ticker_360_context'];row=context['by_ticker']['TEST'];self.assertEqual(row['reported_coverage_count'],3);self.assertIsNone(row['coverage_count']);self.assertFalse(row['independence_verified']);self.assertEqual(row['additional_independent_votes'],0)
if __name__=='__main__':unittest.main(verbosity=2)
