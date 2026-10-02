from pathlib import Path
from datetime import datetime,timezone
from unittest.mock import Mock
import copy,io,json,runpy,types,unittest
W=Path(__file__).resolve().parents[1]/'aws/shared';M=types.SimpleNamespace(**runpy.run_path(str(W/'crypto_stablecoin_observations.py')));T=runpy.run_path(str(W/'crypto_stablecoin_transport.py'))
STAMP=datetime(2020,1,1,tzinfo=timezone.utc)
RAW=b'{"peggedAssets":[{"id":"test","pegType":"peggedUSD","circulating":{"peggedUSD":0}}]}'
class Response(io.BytesIO):
 def __init__(self,raw=RAW,status=200,length=None,url=None):
  super().__init__(raw);self.status=status;self.headers={} if length is None else {'Content-Length':length};self.url=url or T['URL']
 def geturl(self):return self.url
 def read(self,count):return super().read(min(count,3))
def acquire(response):return T['acquire'](Mock(return_value=response),M,now=lambda:STAMP)
class Transport(unittest.TestCase):
 def test_complete_short_chunks_preserve_whole_source_and_actual_zero(self):
  response=Response(length=str(len(RAW)));out=acquire(response)
  self.assertTrue(response.closed);self.assertEqual(out['stablecoins'][0]['snapshots']['current']['value'],0)
  self.assertEqual(T['project'](out['source_attempt'],M),out)
 def test_existing_single_endpoint_headers_timeout_and_no_retries(self):
  opener=Mock(side_effect=TimeoutError('private exception text'));out=T['acquire'](opener,M,now=lambda:STAMP)
  self.assertEqual(opener.call_count,1);request=opener.call_args.args[0]
  self.assertEqual(request.full_url,T['URL']);self.assertEqual(opener.call_args.kwargs,{'timeout':15});self.assertEqual(out['status'],'unavailable');self.assertNotIn('private',json.dumps(out))
 def test_non_success_or_redirect_never_promotes_a_valid_body(self):
  for response in [Response(status=500),Response(url='https://foreign.invalid'),Response(status=True)]:
   out=acquire(response);self.assertEqual(out['status'],'unavailable');self.assertEqual(out['stablecoins'],[]);self.assertEqual(out['source_attempt']['received_bytes'],len(RAW));self.assertTrue(response.closed)
 def test_length_mismatch_and_oversize_remain_retained_prefixes(self):
  out=acquire(Response(length='1'));self.assertFalse(out['source_attempt']['response_complete']);self.assertEqual(out['source_attempt']['received_bytes'],len(RAW))
  small=types.SimpleNamespace(**vars(M));small.LIMIT=12
  out=T['acquire'](Mock(return_value=Response()),small,now=lambda:STAMP)
  self.assertFalse(out['source_attempt']['response_complete']);self.assertEqual(out['source_attempt']['received_bytes'],13);self.assertEqual(out['status'],'unavailable')
 def test_interrupted_body_keeps_received_prefix_and_closes(self):
  class Interrupted(Response):
   def read(self,n):
    if self.tell():raise OSError('private error')
    return super().read(n)
  response=Interrupted();out=acquire(response);self.assertTrue(response.closed);self.assertEqual(out['source_attempt']['received_bytes'],3);self.assertFalse(out['source_attempt']['response_complete'])
 def test_empty_complete_response_remains_unavailable(self):
  out=acquire(Response(b'',length='0'));self.assertTrue(out['source_attempt']['response_complete']);self.assertEqual(out['status'],'unavailable');self.assertEqual(out['source_attempt']['received_bytes'],0)
 def test_tampered_identity_bytes_size_clock_and_types_fail_replay(self):
  attempt=acquire(Response())['source_attempt']
  for key,value in [('request_url','https://foreign.invalid'),('received_sha256','a'*64),('received_bytes',True),('response_complete',1),('acquisition_completed_at','2019-01-01T00:00:00Z'),('declared_content_length','1'),('http_status',True)]:
   with self.subTest(key=key),self.assertRaises(ValueError):T['project']({**attempt,key:value},M)
 def test_all_unavailable_cases_keep_denied_permissions(self):
  for response in [Response(),Response(b'{}'),Response(status=503),Response(b'')]:
   out=acquire(response)
   self.assertTrue(all(out[k]==v for k,v in M.DENIED.items()));self.assertFalse(out['point_in_time_qualified'])
if __name__=='__main__':unittest.main(verbosity=2)
