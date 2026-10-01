from pathlib import Path
from datetime import datetime,timezone
from unittest.mock import Mock
import ast,copy,hashlib,json,math,runpy,unittest
import sys
R=Path(__file__).resolve().parents[1];D=R/'tests/fixtures/crypto-market-cap';sys.path[:0]=[str(R/'aws/shared'),str(R/'aws/lambdas/justhodl-crypto-cycle-risk/source')]
MODULE=runpy.run_path(str(R/'aws/shared/crypto_market_cap_extension.py'));build=MODULE['build_market_cap_extension'];project=MODULE['reported_extension']
def document(last=10):return {'unit':'USD','name':'Invented market capitalization','description':'No realized capitalization in this fixture','values':[{'x':1577836800+i*86400,'y':1 if i<29 else last,'untouched':'row '+str(i)} for i in range(30)]}
def previous(doc):
 path=D/'before/aws/lambdas/justhodl-crypto-intel/source/lambda_function.py.txt';tree=ast.parse(path.read_text(encoding='utf-8'));node=next(n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name=='fetch_mvrv');env={'http_get':Mock(return_value=doc),'fmt':lambda value:'$'+str(value)};exec(compile(ast.Module(body=[node],type_ignores=[]),str(path),'exec'),env);return env['fetch_mvrv']()
class Semantics(unittest.TestCase):
 def test_previous_calls_extension_overvalued_without_realized_capitalization(self):
  out=previous(document());self.assertGreater(out['mvrv_approx'],3);self.assertEqual(out['signal'],'OVERVALUED')
 def test_exact_named_mean_and_latest_observation_pointer(self):
  out=build(document());m=out['market_cap_extension'];self.assertAlmostEqual(m['value'],10/1.3);self.assertEqual(m['denominator']['count'],30);self.assertEqual(m['numerator']['source_row'],'/values/29');self.assertEqual(m['returned_rows'],30)
 def test_no_mvrv_or_valuation_permission_even_for_large_extension(self):
  out=build(document());self.assertIsNone(out['mvrv_approx']);self.assertEqual(out['signal'],'UNAVAILABLE');self.assertIsNone(out['momentum_30d'])
  for obj in [out,out['market_cap_extension']]:
   for k in ['calls_eligible','sizing_eligible','execution_eligible','forecast_qualified']:self.assertIs(obj[k],False)
   self.assertEqual(obj['independent_investment_votes'],0)
 def test_explicit_zero_is_not_missing(self):
  out=build(document(0));self.assertEqual(out['market_cap'],0);self.assertEqual(out['market_cap_extension']['value'],0);self.assertEqual(project(out),0)
 def test_complete_parsed_response_preserved_without_mutation(self):
  d=document();saved=copy.deepcopy(d);out=build(d);self.assertEqual(d,saved);self.assertEqual(json.loads(out['source_response_json']),d);self.assertEqual(out['source_response_sha256'],hashlib.sha256(out['source_response_json'].encode()).hexdigest());self.assertIn('not_original_http_bytes',out['source_response_kind'])
 def test_units_required_before_claiming_usd(self):
  for unit in [None,'BTC','usd',True,{}]:
   d=document();d['unit']=unit;out=build(d);self.assertEqual(out['market_cap_extension']['reason'],'source_unit_missing_or_not_usd');self.assertIsNone(out['market_cap_extension']['value'])
 def test_every_row_required_no_partial_success(self):
  for value in [None,True,False,'1',float('nan'),float('inf'),-1]:
   d=document();d['values'][17]['y']=value;out=build(d);self.assertEqual(out['status'],'unavailable');self.assertIsNone(project(out));self.assertEqual(out['market_cap_extension']['returned_rows'],30);json.dumps(out,allow_nan=False)
 def test_invalid_order_and_clock_fail_whole_series(self):
  for clock in [None,True,1577836800.0,1577836800,-1,10**30]:
   d=document();d['values'][17]['x']=clock;out=build(d);self.assertEqual(out['status'],'unavailable');self.assertIsNone(out['market_cap_extension']['value'])
 def test_sample_is_not_declared_daily_or_fresh(self):
  d=document();d['values'][-1]['x']+=86400*20;out=build(d);m=out['market_cap_extension'];self.assertEqual(m['status'],'descriptive');self.assertFalse(m['observation_window']['regular_daily_sampling_verified']);self.assertFalse(m['observation_freshness_verified']);self.assertFalse(m['first_release_availability_verified']);self.assertFalse(m['source_definition_verified'])
 def test_insufficient_population_and_zero_denominator_withheld(self):
  d=document();d['values'].pop();self.assertEqual(build(d)['market_cap_extension']['reason'],'fewer_than_30_returned_observations')
  d=document();[r.update(y=0) for r in d['values']];self.assertEqual(build(d)['market_cap_extension']['reason'],'unavailable_nonzero_denominator')
 def test_overflow_and_underflow_cannot_report_valid_ratio(self):
  d=document();[r.update(y=1e308) for r in d['values']];self.assertEqual(build(d)['status'],'unavailable')
  d=document(5e-324);[r.update(y=1e200) for r in d['values'][:-1]];self.assertEqual(build(d)['market_cap_extension']['reason'],'unrepresentable_ratio')
 def test_consumer_refuses_all_legacy_only_values(self):
  for value in [0,1,4,None,'4']:
   self.assertIsNone(project({'mvrv_approx':value,'mvrv':value}))
 def test_consumer_refuses_invalid_contract_permissions_and_types(self):
  for key,value in [('contract','other'),('unit','usd'),('status','ok'),('calls_eligible',True),('independent_investment_votes',False),('value','4'),('value',float('nan'))]:
   out=build(document());out['market_cap_extension'][key]=value;self.assertIsNone(project(out))
 def test_nonfinite_source_tokens_are_retained_only_inside_a_string(self):
  d=document();d['values'][10]['y']=float('nan');out=build(d);self.assertTrue(math.isnan(json.loads(out['source_response_json'])['values'][10]['y']));self.assertEqual(json.loads(json.dumps(out,allow_nan=False))['status'],'unavailable')
 def test_boundary_withholds_composite_and_proxy_vote_without_losing_prior_quality(self):
  original={'dump_risk_score':80,'risk_level':'HIGH','action':'TRIM','quality':{'status':'bond_unqualified'},'factors':{'mvrv_extension':{'risk':50,'mvrv':None,'weight':.1},'other':{'risk':70}},'top_drivers':[{'risk':70}],'methodology':'legacy','honesty_note':'legacy note'};snapshot=copy.deepcopy(original)
  out=MODULE['apply_crypto_proxy_boundary'](original,build(document()));self.assertEqual(original,snapshot);self.assertIsNone(out['dump_risk_score']);self.assertIsNone(out['factors']['mvrv_extension']['risk']);self.assertEqual(out['decision']['verb'],'WAIT');self.assertFalse(out['calls_eligible']);self.assertEqual(out['quality']['preceding_quality'],snapshot['quality']);self.assertEqual(out['unqualified_legacy_mvrv_factor'],snapshot['factors']['mvrv_extension']);self.assertEqual(out['factors']['other'],snapshot['factors']['other'])

def source(function):return R/'aws/lambdas'/function/'source/lambda_function.py'
def load_function(function,name,scope,index=0):
    path=source(function);nodes=[n for n in ast.parse(path.read_text(encoding='utf-8')).body if isinstance(n,ast.FunctionDef) and n.name==name]
    exec(compile(ast.Module(body=[nodes[index]],type_ignores=[]),str(path),'exec'),scope)
    return scope[name]

class Consumers(unittest.TestCase):
    def test_producer_calls_existing_source_once_and_retains_response(self):
        d=document(0);get=Mock(return_value=d)
        out=load_function('justhodl-crypto-intel','fetch_mvrv',{'http_get':get,'fmt':lambda x:str(x)})()
        self.assertEqual(get.call_count,1);self.assertEqual(out['market_cap'],0);self.assertEqual(json.loads(out['source_response_json']),d);self.assertIsNone(out['mvrv_approx'])
    def test_financial_reader_never_projects_legacy_mvrv_but_keeps_new_ratio(self):
        import io
        for ratios in [build(document(0)),{'mvrv_approx':9,'signal':'OVERVALUED'}]:
            def get(**kw):return {'Body':io.BytesIO(json.dumps({'onchain_ratios':ratios} if kw['Key']=='crypto-intel.json' else {}).encode())}
            fn=load_function('justhodl-financial-secretary','fetch_tier2',{'s3':type('Store',(),{'get_object':staticmethod(get)})(),'json':json,'BUCKET':'invented'})
            out=fn()['crypto'];self.assertIsNone(out['mvrv_approx']);self.assertEqual(out['market_cap_to_mean_ratio'],0 if 'market_cap_extension' in ratios else None)
    def test_telegram_enricher_cannot_reintroduce_legacy_vote(self):
        for ratios in [build(document(0)),{'mvrv_approx':9,'signal':'OVERVALUED'}]:
            fn=load_function('justhodl-telegram-bot','enrich_with_crypto_intel',{'get_crypto_intel':lambda:{'onchain_ratios':ratios}})
            out=fn({'retained':True});self.assertTrue(out['retained']);self.assertIsNone(out['mvrv']);self.assertEqual(out['onchain_signal'],'UNAVAILABLE');self.assertEqual(out['market_cap_to_mean_ratio'],0 if 'market_cap_extension' in ratios else None)
    def test_morning_actual_projection_fields_withhold_signal_and_false_30d(self):
        tree=ast.parse(source('justhodl-morning-intelligence').read_text(encoding='utf-8'))
        fn=next(n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name=='extract_metrics')
        expr=next(n.value for n in fn.body if isinstance(n,ast.Return))
        fields={k.value:v for k,v in zip(expr.keys,expr.values) if isinstance(k,ast.Constant)}
        for ratios in [build(document(0)),{'mvrv_approx':9,'signal':'OVERVALUED','momentum_30d':200}]:
            out={key:eval(compile(ast.Expression(fields[key]),'actual-morning-projection','eval'),{'oc':ratios}) for key in ('mvrv','market_cap_to_mean_ratio','onchain_signal','onchain_momentum')}
            self.assertIsNone(out['mvrv']);self.assertIsNone(out['onchain_momentum']);self.assertEqual(out['onchain_signal'],'UNAVAILABLE');self.assertEqual(out['market_cap_to_mean_ratio'],0 if 'market_cap_extension' in ratios else None)
    def test_chat_actual_crypto_context_names_proxy_and_preserves_zero(self):
        import textwrap
        s=source('justhodl-ai-chat').read_text(encoding='utf-8');start=s.index('            ratio = __import__("crypto_market_cap_extension")');end=s.index("        ef = get_s3('data/exchange-flows.json')",start)
        for ratios in [build(document(0)),{'mvrv_approx':9}]:
            env={'cd':{'onchain_ratios':ratios},'lines':[]};exec(compile(textwrap.dedent(s[start:end]),'actual-chat-context','exec'),env)
            text=env['lines'][0];self.assertIn('MVRV unavailable; no valuation vote',text);self.assertNotIn('[ON-CHAIN MVRV]',text);self.assertIn('0.0;' if 'market_cap_extension' in ratios else 'Unavailable;',text)
    def test_paid_helpers_return_before_transport_or_credentials(self):
        from unittest.mock import patch
        for function,args in [('justhodl-financial-secretary',('invented prompt',)),('justhodl-telegram-bot',('invented question',{'sensitive':'invented only'}))]:
            # No globals exist for credentials or HTTP: any access would fail.
            fn=load_function(function,'ask_claude',{})
            with patch('urllib.request.urlopen',side_effect=AssertionError('No network')) as transport:
                self.assertEqual(fn(*args),'Model commentary unavailable under the no-paid-API policy.');transport.assert_not_called()
    def test_complete_cycle_handler_withholds_publication_and_alert_vote(self):
        from datetime import date
        import time
        from unittest.mock import patch
        for ratios in [build(document()),build(document(0)),{'mvrv_approx':4.0,'signal':'OVERVALUED'},{}]:
            saved={};alert=Mock()
            def put(**kw):saved[kw['Key']]=json.loads(kw['Body'])
            scope={'json':json,'date':date,'datetime':datetime,'timezone':timezone,'time':time,'HALVINGS':[],'FED_TRANSITIONS':[],
                   'days_since':lambda *_:365,'days_to_inflation_print':lambda *_:9,'macro_risk_factor':lambda:(60,{}),
                   'read_json':lambda key:{'onchain_ratios':ratios} if key=='crypto-intel.json' else {},
                   's3':type('Store',(),{'put_object':staticmethod(put)})(),'BUCKET':'invented','OUT_KEY':'invented/cycle.json','_telegram':alert}
            fn=load_function('justhodl-crypto-cycle-risk','lambda_handler',scope)
            with patch('urllib.request.urlopen',side_effect=AssertionError('Offline transport')):response=fn()
            out=saved['invented/cycle.json'];self.assertEqual(response['statusCode'],200);self.assertIsNone(out['dump_risk_score']);self.assertIsNone(out['factors']['mvrv_extension']['risk']);self.assertEqual(out['decision']['verb'],'WAIT');self.assertFalse(out['calls_eligible']);self.assertFalse(out['sizing_eligible']);self.assertFalse(out['execution_eligible']);self.assertFalse(out['forecast_qualified']);alert.assert_not_called();self.assertEqual(out['top_drivers'],[])
    def test_whole_predecessors_and_old_preservation_manifests_remain_exact(self):
        plan=json.loads((D/'edits.json').read_bytes())
        for p,rec in plan.items():
            raw=(D/('before/'+p+'.txt')).read_bytes();self.assertEqual(hashlib.sha256(raw).hexdigest(),rec['predecessor_sha256']);s=raw.decode('utf-8')
            for a,b in rec['edits']:self.assertEqual(s.count(a),1);s=s.replace(a,b)
            self.assertEqual(s,(R/p).read_text(encoding='utf-8'));self.assertEqual(hashlib.sha256(s.encode()).hexdigest(),rec['candidate_sha256'])

if __name__=='__main__':unittest.main(verbosity=2)

