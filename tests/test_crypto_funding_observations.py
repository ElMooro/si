from pathlib import Path
from base64 import b64decode
import hashlib,json,runpy,unittest
from datetime import datetime,timezone
import ast,sys
R=Path(__file__).resolve().parents[1];sys.path.insert(0,str(R/'aws/shared'))
M=runpy.run_path(str(R/'aws/shared/crypto_funding_observations.py'));P=M['point']
def body(rate='0.0001',**extra):return json.dumps({'code':'0','data':[{'instId':'BTC-USDT-SWAP','fundingRate':rate,'fundingTime':'1577836800000','nextFundingTime':'1577840400000','ts':'1577836799000',**extra}]}).encode()
class Points(unittest.TestCase):
 def test_epoch_milliseconds_do_not_gain_float_rounding(self):
  out=P('okx','BTC-USDT-SWAP',body(fundingTime='253402300799999',nextFundingTime='253402300799999'))
  self.assertEqual(out['reported_settlement_at'],'9999-12-31T23:59:59.999000+00:00');self.assertFalse(out['source_timing_qualified'])
 def test_zero_remains_zero_and_has_no_short_bias(self):
  out=P('okx','BTC-USDT-SWAP',body('0'));self.assertEqual(out['funding_rate'],0);self.assertEqual(out['funding_payment_direction'],'zero_reported_rate');self.assertFalse(out['calls_eligible'])
 def test_decimal_and_percent_units_remain_explicit(self):
  out=P('okx','BTC-USDT-SWAP',body());self.assertEqual(out['funding_rate_decimal'],'0.0001');self.assertEqual(out['funding_rate_pct'],.01);self.assertEqual(out['funding_rate_pct_decimal'],'0.0100')
 def test_missing_nonfinite_boolean_or_lossy_input_never_creates_zero(self):
  for value in [None,True,False,0,{},[],'NaN','Infinity','0.100000000000000000001','1e-999']:
   out=P('okx','BTC-USDT-SWAP',body(value));self.assertEqual(out['status'],'unavailable');self.assertIsNone(out['funding_rate']);json.dumps(out,allow_nan=False)
 def test_one_hour_schedule_is_retained_without_annualization(self):
  out=P('okx','BTC-USDT-SWAP',body());self.assertEqual(out['reported_schedule_difference_ms'],3600000);self.assertIsNone(out['annualized_pct']);self.assertFalse(out['source_timing_qualified'])
 def test_complete_original_bytes_remain_replayable_even_when_rejected(self):
  for raw in [body(),body(None),b'{"code":"0","code":"1"}',b'{"bad":NaN}',b'bad']:
   out=P('okx','BTC-USDT-SWAP',raw);self.assertEqual(b64decode(out['original_response_base64']),raw);self.assertEqual(out['original_response_sha256'],hashlib.sha256(raw).hexdigest());self.assertEqual(out['original_response_bytes'],len(raw))
 def test_instrument_and_settlement_clock_required(self):
  for kw in [{'instId':'OTHER'},{'fundingTime':None},{'fundingTime':True},{'fundingTime':'999999999999999'}]:self.assertEqual(P('okx','BTC-USDT-SWAP',body(**kw))['status'],'unavailable')
 def test_bybit_history_keeps_settled_basis_separate(self):
  raw=json.dumps({'retCode':0,'result':{'category':'linear','list':[{'symbol':'BTCUSDT','fundingRate':'-0.0001','fundingRateTimestamp':'1577836800000'}]},'time':1577836900000}).encode();out=P('bybit','BTCUSDT',raw);self.assertEqual(out['rate_basis'],'last_settled_history_rate');self.assertEqual(out['funding_payment_direction'],'shorts_pay_longs');self.assertIsNone(out['reported_schedule_difference_ms']);self.assertIsNone(out['annualized_pct'])

class Response:
 def __init__(self,raw,status=200,length=None):
  import io
  self.stream=io.BytesIO(raw);self.status=status;self.headers={'Content-Length':str(len(raw)) if length is None else length}
 def read(self,n):return self.stream.read(n)
 def __enter__(self):return self
 def __exit__(self,*args):pass

def transport_factory(rate='0.0001',only=None,missing=None):
 from urllib.parse import urlsplit,parse_qs
 from unittest.mock import Mock
 def open_(req,timeout):
  q=parse_qs(urlsplit(req.full_url).query);inst=(q.get('instId') or q.get('symbol'))[0]
  if only=='bybit' and 'instId' in q:return Response(b'{"code":"1"}')
  value=None if inst==missing else rate
  if 'instId' in q:return Response(body(value,instId=inst))
  return Response(json.dumps({'retCode':0,'result':{'category':'linear','list':[{'symbol':inst,'fundingRate':value,'fundingRateTimestamp':'1577836800000'}]},'time':1577836900000}).encode())
 return Mock(side_effect=open_)

class Collection(unittest.TestCase):
 def test_actual_zero_does_not_trigger_fallback_or_short_bias(self):
  opener=transport_factory('0');out=M['collect_funding'](opener)
  self.assertEqual(opener.call_count,10);self.assertEqual(out['observed_instruments'],10)
  self.assertEqual(out['selected_provider'],'okx');self.assertIsNone(out['avg_rate_pct']);self.assertIsNone(out['avg_funding'])
  self.assertEqual(out['leverage_sentiment'],'UNAVAILABLE');self.assertIsNone(out['long_count'])
  self.assertTrue(all(row['funding_payment_direction']=='zero_reported_rate' for row in out['rates']))
 def test_failed_primary_requests_are_retained_separately_from_history(self):
  opener=transport_factory(only='bybit');out=M['collect_funding'](opener)
  self.assertEqual(opener.call_count,20);self.assertEqual(len(out['source_attempts']),20)
  self.assertEqual(out['selected_provider'],'bybit')
  for row in out['rates']:self.assertEqual(row['rate_basis'],'last_settled_history_rate');self.assertGreaterEqual(row['evidence_index'],10);self.assertIsNone(row['annualized_pct'])
 def test_partial_primary_is_not_silently_filled_from_other_event_basis(self):
  opener=transport_factory(missing='BTC-USDT-SWAP');out=M['collect_funding'](opener)
  self.assertEqual(opener.call_count,10);self.assertEqual(out['observed_instruments'],9);self.assertEqual(out['unavailable_instruments'],1)
  self.assertEqual(len(out['rates']),10);self.assertEqual(out['rates'][0]['status'],'unavailable');self.assertIsNone(out['rates'][0]['funding_rate'])
 def test_replay_exact_originals_produces_each_reported_point(self):
  for source in [None,'bybit']:
   out=M['collect_funding'](transport_factory(only=source))
   for attempt in out['source_attempts']:
    raw=b64decode(attempt['original_response_base64']);self.assertEqual(len(raw),attempt['original_response_bytes'])
    replay=P(attempt['provider'],attempt['instrument'],raw)
    for key,value in replay.items():self.assertEqual(value,attempt[key])
 def test_capture_limit_prefix_is_never_called_complete_original(self):
  from unittest.mock import Mock
  out=M['acquire']('okx','BTC-USDT-SWAP',Mock(return_value=Response(b'x'*1_000_002)),lambda:datetime(2020,1,1,tzinfo=timezone.utc))
  self.assertFalse(out['response_complete']);self.assertIsNone(out['original_response_sha256']);self.assertEqual(out['received_prefix_bytes'],1_000_001)
 def test_content_length_mismatch_stays_incomplete(self):
  from unittest.mock import Mock
  out=M['acquire']('okx','BTC-USDT-SWAP',Mock(return_value=Response(body(),length='999999')),lambda:datetime(2020,1,1,tzinfo=timezone.utc))
  self.assertFalse(out['response_complete']);self.assertEqual(out['reason'],'declared_content_length_mismatch');self.assertEqual(b64decode(out['received_prefix_base64']),body())
 def test_read_interruption_retains_prefix_and_marks_incomplete(self):
  from http.client import IncompleteRead
  from unittest.mock import Mock
  r=Response(b'');r.read=Mock(side_effect=[b'first',IncompleteRead(b'second',99)])
  out=M['acquire']('okx','BTC-USDT-SWAP',Mock(return_value=r),lambda:datetime(2020,1,1,tzinfo=timezone.utc))
  self.assertEqual(out['reason'],'IncompleteRead');self.assertEqual(b64decode(out['received_prefix_base64']),b'firstsecond');self.assertIsNone(out['original_response_bytes'])
 def test_http_error_cannot_be_promoted_even_with_valid_json_body(self):
  from unittest.mock import Mock
  out=M['acquire']('okx','BTC-USDT-SWAP',Mock(return_value=Response(body(),status=500)),lambda:datetime(2020,1,1,tzinfo=timezone.utc))
  self.assertEqual(out['status'],'unavailable');self.assertIsNone(out['funding_rate']);self.assertTrue(out['response_complete']);self.assertEqual(b64decode(out['original_response_base64']),body())
 def test_invalid_all_sources_never_create_nan_zero_or_aggregate(self):
  out=M['collect_funding'](transport_factory('NaN'));self.assertEqual(out['observed_instruments'],0);self.assertEqual(len(out['source_attempts']),20);self.assertEqual(out['status'],'unavailable');json.dumps(out,allow_nan=False)
 def test_exceptions_do_not_stop_remaining_source_observations(self):
  from unittest.mock import Mock
  opener=Mock(side_effect=TimeoutError('invented'));out=M['collect_funding'](opener)
  self.assertEqual(opener.call_count,20);self.assertEqual(out['observed_instruments'],0);self.assertEqual(len(out['rates']),10)
  self.assertTrue(all(a['transport_error']=='TimeoutError' for a in out['source_attempts']));json.dumps(out,allow_nan=False)

class Projection(unittest.TestCase):
 def test_context_preserves_exact_zero_without_investment_authority(self):
  packet=M['collect_funding'](transport_factory('0'));out=M['funding_context'](packet)
  self.assertEqual(len(out['rates']),10);self.assertEqual(out['rates'][0]['funding_rate_pct'],0)
  self.assertIn('zero reported rate',M['funding_line'](out['rates'][0]));self.assertFalse(out['calls_eligible'])
 def test_legacy_rows_never_reenter_context(self):
  out=M['funding_context']({'rates':[{'symbol':'BTC','funding_rate_pct':3,'annualized_pct':3285}]})
  self.assertEqual(out['rates'],[]);self.assertEqual(out['status'],'unavailable')
 def test_mutated_units_numeric_projection_permissions_and_duplicates_refused(self):
  import copy
  row=M['collect_funding'](transport_factory())['rates'][0]
  for key,value in [('funding_rate',True),('funding_rate_pct','0.01'),('funding_rate_pct',float('inf')),('funding_rate_pct_decimal','0.0100000000000000001'),('unit','percent'),('annualized_pct',10.95),('calls_eligible',True),('independent_investment_votes',False),('funding_payment_direction','SHORT'),('reported_settlement_at','2020-01-01'),('instrument','ETH-USDT-SWAP')]:
   bad=copy.deepcopy(row);bad[key]=value;self.assertIsNone(M['reported_point'](bad),key)
  packet=M['collect_funding'](transport_factory());packet['rates'].append(copy.deepcopy(packet['rates'][0]));self.assertEqual(M['funding_context'](packet)['rates'],[])
 def test_risk_boundary_does_not_lose_prior_evidence_or_mutate_input(self):
  import copy
  old={'score':50,'regime':'MODERATE','action':'ACCUMULATE','signals':['invented'],'quality':{'status':'prior'}};saved=copy.deepcopy(old)
  out=M['apply_funding_risk_boundary'](old,{});self.assertEqual(old,saved);self.assertEqual(out['unqualified_legacy_risk'],saved);self.assertIsNone(out['score']);self.assertEqual(out['decision']['verb'],'WAIT');self.assertEqual(out['quality']['preceding_quality'],saved['quality'])


def actual_function(function,name,scope):
 path=R/'aws/lambdas'/function/'source/lambda_function.py'
 n=next(n for n in ast.parse(path.read_text(encoding='utf-8')).body if isinstance(n,ast.FunctionDef) and n.name==name)
 exec(compile(ast.Module(body=[n],type_ignores=[]),str(path),'exec'),scope)
 return scope[name]

class Consumers(unittest.TestCase):
 def test_actual_collector_uses_only_existing_endpoints_and_no_extra_requests(self):
  import types
  opener=transport_factory('0');fn=actual_function('justhodl-crypto-intel','fetch_funding',{'urllib':types.SimpleNamespace(request=types.SimpleNamespace(urlopen=opener))})
  out=fn();self.assertEqual(opener.call_count,10);self.assertEqual(out['rates'][0]['funding_rate'],0)
  self.assertTrue(all(call.kwargs['timeout']==8 for call in opener.call_args_list))
 def test_actual_risk_cannot_impute_missing_funding_as_zero_or_trade(self):
  fn=actual_function('justhodl-crypto-intel','risk',{})
  for funding in [{},None,{'avg_funding':float('nan')},{'avg_funding':9},M['collect_funding'](transport_factory('0'))]:
   out=fn({},funding,{}, {}, {});self.assertIsNone(out['score']);self.assertEqual(out['action'],'WAIT');self.assertFalse(out['calls_eligible']);self.assertFalse(out['sizing_eligible']);self.assertEqual(out['signals'],[]);json.dumps(out,allow_nan=False)
 def test_financial_context_rejects_legacy_funding_and_keeps_typed_zero(self):
  import io,types
  for funding in [{'rates':[{'funding_rate_pct':5}]},M['collect_funding'](transport_factory('0'))]:
   get=lambda **kw:{'Body':io.BytesIO(json.dumps({'funding':funding} if kw['Key']=='crypto-intel.json' else {}).encode())}
   out=actual_function('justhodl-financial-secretary','fetch_tier2',{'s3':types.SimpleNamespace(get_object=get),'json':json,'BUCKET':'invented'})()['crypto']['funding_summary']
   self.assertEqual(len(out['rates']),10 if 'contract' in funding else 0);self.assertFalse(out['calls_eligible'])
 def test_telegram_enricher_keeps_zero_but_refuses_legacy_rows(self):
  for funding in [{'rates':[{'funding_rate_pct':5}]},M['collect_funding'](transport_factory('0'))]:
   fn=actual_function('justhodl-telegram-bot','enrich_with_crypto_intel',{'get_crypto_intel':lambda:{'funding':funding}})
   out=fn({'unrelated':'kept'});self.assertEqual(out['unrelated'],'kept');self.assertEqual(len(out['funding_rates']),5 if 'contract' in funding else 0)
 def test_telegram_render_never_crashes_on_unavailable_rate_or_rounds_tiny_to_zero(self):
  from unittest.mock import Mock
  row=M['collect_funding'](transport_factory('0.000000000001'))['rates'][0];send=Mock()
  ctx={'send_typing':Mock(),'enrich_with_crypto_intel':lambda _:{'funding_rates':[row,{'funding_rate_pct':None}]},'parse_report':lambda _: {},'get_report':lambda: {},'send_message':send,'n':lambda value,*args:str(value),'fear_label':lambda value:str(value)}
  actual_function('justhodl-telegram-bot','cmd_crypto',ctx)(0);text=send.call_args.args[1]
  self.assertIn('1.00E-10% per reported event',text);self.assertIn('Funding observation unavailable',text);self.assertNotIn('(LONG)',text);self.assertEqual(send.call_count,1)
 def test_morning_projection_keeps_event_basis_and_null_annualization(self):
  source=R/'aws/lambdas/justhodl-morning-intelligence/source/lambda_function.py';tree=ast.parse(source.read_text(encoding='utf-8'))
  fn=next(n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name=='extract_metrics');ret=next(n.value for n in fn.body if isinstance(n,ast.Return));fields={k.value:v for k,v in zip(ret.keys,ret.values) if isinstance(k,ast.Constant)}
  btc=M['collect_funding'](transport_factory('0'))['rates'][0]
  out={k:eval(compile(ast.Expression(fields[k]),'invented-morning-funding','eval'),{'btc_fund':btc}) for k in ['btc_funding_pct','btc_funding_annual','btc_funding_observation','btc_sentiment']}
  self.assertEqual(out['btc_funding_pct'],0);self.assertIsNone(out['btc_funding_annual']);self.assertEqual(out['btc_sentiment'],'UNAVAILABLE');self.assertIn('zero reported rate',out['btc_funding_observation'])
 def test_cycle_factor_has_no_unequal_period_average_or_neutral_imputation(self):
  import textwrap
  source=(R/'aws/lambdas/justhodl-crypto-cycle-risk/source/lambda_function.py').read_text(encoding='utf-8')
  start=source.index('    from crypto_funding_observations import funding_context');end=source.index('\n\n',source.index('    factors["funding_leverage"]',start));env={'crypto':{'funding':M['collect_funding'](transport_factory())},'factors':{}}
  exec(compile(textwrap.dedent(source[start:end]),'actual-funding-factor','exec'),env);out=env['factors']['funding_leverage'];self.assertIsNone(out['risk']);self.assertIsNone(out['avg_funding_pct']);self.assertEqual(len(out['reported_observations']['rates']),10)
 def test_complete_predecessors_and_every_exact_edit_are_retained(self):
  d=R/'tests/fixtures/crypto-funding-observations';plans=json.loads((d/'edits.json').read_bytes())
  for path,plan in plans.items():
   raw=(d/('before/'+path+'.txt')).read_bytes();self.assertEqual(hashlib.sha256(raw).hexdigest(),plan['predecessor_sha256']);text=raw.decode('utf-8')
   for before,after in plan['edits']:self.assertEqual(text.count(before),1);text=text.replace(before,after)
   self.assertEqual(text.encode(),(R/path).read_bytes());self.assertEqual(hashlib.sha256(text.encode()).hexdigest(),plan['candidate_sha256'])

if __name__=='__main__':unittest.main(verbosity=2)


