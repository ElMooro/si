from pathlib import Path
from datetime import datetime,timezone
from unittest.mock import Mock
from urllib.error import HTTPError
import base64,copy,io,json,runpy,types,unittest
W=Path(__file__).resolve().parents[1]/'aws/shared';M=types.SimpleNamespace(**runpy.run_path(str(W/'crypto_sentiment_observations.py')));T=runpy.run_path(str(W/'crypto_sentiment_transport.py'))
NOW=datetime(2020,1,1,tzinfo=timezone.utc)
RAW=b'{"name":"Fear and Greed Index","data":[{"value":"0","timestamp":"1577836800"}],"metadata":{"error":null}}'
class Response(io.BytesIO):
 def __init__(self,raw,url,status=200,headers=None):super().__init__(raw);self.url=url;self.status=status;self.headers=headers or {}
 def geturl(self):return self.url
def opener(request,timeout):return Response(RAW if 'alternative' in request.full_url else b'{"prices":[]}',request.full_url)
def collect(open=opener,remaining=lambda:180000):return T['collect'](open,M,remaining,now=lambda:NOW)
class Transport(unittest.TestCase):
 def test_exact_three_existing_requests_and_timeouts_without_retries(self):
  called=Mock(side_effect=opener);out=collect(called)
  self.assertEqual([(call.args[0].full_url,call.kwargs['timeout']) for call in called.call_args_list],list(T['REQUESTS']))
  self.assertEqual(out['recent_reported_observations']['current'],0);self.assertEqual(out['full_reported_observations']['current'],0)
  self.assertEqual(out['current'],0);self.assertFalse(out['synthetic_history_is_provider_data']);self.assertEqual(base64.b64decode(out['source_attempts'][2]['received_base64']),b'{"prices":[]}')
 def test_short_chunks_are_not_eof_and_every_stream_closes(self):
  saved=[]
  class Short(Response):
   def read(self,n):return super().read(min(n,3))
  def open(request,timeout):response=Short(RAW,request.full_url);saved.append(response);return response
  out=collect(open);self.assertTrue(all(r.closed for r in saved));self.assertTrue(all(a['response_complete'] for a in out['source_attempts']))
  self.assertEqual(base64.b64decode(out['source_attempts'][0]['received_base64']),RAW)
 def test_error_status_body_is_retained_without_observation(self):
  def open(request,timeout):raise HTTPError(request.full_url,429,'invented rate limit',{},io.BytesIO(b'original error'))
  out=collect(open);self.assertEqual(out['recent_reported_observations']['status'],'unavailable')
  self.assertEqual(base64.b64decode(out['source_attempts'][0]['received_base64']),b'original error');self.assertEqual(out['source_attempts'][0]['http_status'],429)
 def test_unexpected_redirect_withholds_numbers_even_with_valid_json(self):
  out=collect(lambda request,timeout:Response(RAW,'https://other.invalid'))
  self.assertFalse(out['source_attempts'][0]['final_url_matches_request']);self.assertIsNone(out['recent_reported_observations']['current'])
 def test_read_failure_retains_exact_prefix_and_partial_exception_bytes(self):
  class Broken(Response):
   def read(self,n):
    if self.tell():
     exc=OSError('invented');exc.partial=b'cd';raise exc
    return super().read(2)
  out=collect(lambda request,timeout:Broken(b'abcdef',request.full_url));a=out['source_attempts'][0]
  self.assertEqual(base64.b64decode(a['received_base64']),b'abcd');self.assertFalse(a['response_complete']);self.assertEqual(a['transport_error'],'body_read_failed')
 def test_invalid_or_huge_content_length_is_retained_and_unavailable(self):
  for length in ['1','9'*5000,'-1','bad']:
   out=collect(lambda request,timeout:Response(RAW,request.full_url,headers={'Content-Length':length}));a=out['source_attempts'][0]
   self.assertFalse(a['response_complete']);self.assertEqual(a['declared_content_lengths'],[length]);self.assertEqual(base64.b64decode(a['received_base64']),RAW)
 def test_duplicate_content_length_headers_refuse_complete_claim(self):
  headers=types.SimpleNamespace(get_all=lambda key:[str(len(RAW)),str(len(RAW))]);out=collect(lambda request,timeout:Response(RAW,request.full_url,headers=headers))
  self.assertFalse(out['source_attempts'][0]['response_complete'])
 def test_insufficient_or_invalid_budget_cannot_start_provider_requests(self):
  for budget in [0,29999,True,None,float('inf'),float('nan')]:
   open=Mock();out=collect(open,lambda:budget);open.assert_not_called();self.assertTrue(all(a['transport_error']=='insufficient_remaining_time' for a in out['source_attempts'][:2]));self.assertEqual(out['source_attempts'][2]['transport_error'],'upstream_prerequisite_unavailable')
 def test_budget_can_stop_a_body_with_its_exact_prefix_retained(self):
  budgets=iter([180000,180000,0]);response=Response(b'abcdef',T['REQUESTS'][0][0]);model=types.SimpleNamespace(**vars(M));model.LIMIT=100
  a=T['capture'](Mock(return_value=response),*T['REQUESTS'][0],model,lambda:NOW,lambda:next(budgets))
  self.assertFalse(a['response_complete']);self.assertEqual(a['received_bytes'],6);self.assertEqual(a['transport_error'],'insufficient_remaining_time');self.assertTrue(response.closed)
 def test_replay_rejects_changed_body_proof_or_request_population(self):
  out=collect()
  for edit in [{'received_bytes':True},{'received_sha256':'f'*64},{'received_base64':'!bad'},{'response_complete':1}]:
   attempts=copy.deepcopy(out['source_attempts']);attempts[0].update(edit)
   with self.assertRaises(ValueError):T['project'](attempts,M)
  for attempts in [out['source_attempts'][:2],list(reversed(out['source_attempts']))]:
   with self.assertRaises(ValueError):T['project'](attempts,M)
 def test_conflicting_copies_remain_visible_and_withhold_current(self):
  def open(request,timeout):return Response(RAW.replace(b'"0"',b'"100"') if 'limit=0' in request.full_url else RAW,request.full_url)
  out=collect(open);self.assertIsNone(out['current']);self.assertEqual(out['current_reason'],'endpoint_current_values_disagree');self.assertEqual(len(out['source_disagreements']),1);self.assertFalse(out['endpoint_copies_are_independent_sources'])
 def test_different_latest_clocks_are_not_silently_joined(self):
  def open(request,timeout):return Response(RAW.replace(b'1577836800',b'1577750400') if 'limit=0' in request.full_url else RAW,request.full_url)
  out=collect(open);self.assertIsNone(out['current']);self.assertEqual(out['current_reason'],'endpoint_latest_observation_clocks_differ')
 def test_missing_full_copy_does_not_fabricate_history_or_hide_reported_recent_zero(self):
  def open(request,timeout):return Response(b'{}' if 'limit=0' in request.full_url else RAW,request.full_url)
  out=collect(open);self.assertEqual(out['current'],0);self.assertEqual(out['full_history'],[]);self.assertIsNone(out['avg_7d'])
 def test_conflicting_window_withholds_average_but_keeps_original_arithmetic(self):
  base={'status':'descriptive','current':0,'label':'Invented','series':[{'timestamp':1,'date':'2020-01-01','value':0,'source_rows':['/data/0']},{'timestamp':2,'date':'2020-01-02','value':0,'source_rows':['/data/1']}],
        'averages':{'7d':{'status':'complete_daily_window','value':0,'source_dates':['2020-01-01','2020-01-02'],'sum_index_points':0,'denominator':7}}}
  other=copy.deepcopy(base);other['series'][0]['value']=100
  out=T['reconcile'](base,other);self.assertEqual(out['current'],0);self.assertIsNone(out['avg_7d']);self.assertEqual(out['averages']['7d']['sum_index_points'],0);self.assertTrue(out['averages']['7d']['cross_response_conflict'])
if __name__=='__main__':unittest.main(verbosity=2)
