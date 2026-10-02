from pathlib import Path
from unittest.mock import Mock,patch
import ast,copy,hashlib,importlib.util,json,math,socket,sys,types,unittest
R=Path(__file__).resolve().parents[4]
SOURCE=R/'aws/lambdas/justhodl-ai/source/market_read.py'
sys.path[:0]=[str(R/'aws/lambdas/justhodl-ai/source'),str(R/'aws/shared')]
def doc():
 return {'overall':'Invented offline example without any market claim.','macro':'Invented macro explanation.', 'stocks':{'stance':'SELECTIVE','read':'Invented explanation'},'bonds':{'stance':'NEUTRAL','read':'Invented explanation'},'metals':{'stance':'HOLD','read':'Invented explanation'},'crypto':{'stance':'HOLD','read':'Invented explanation'},'best_opportunities':[], 'calls':[{'ticker':'QAONLY','direction':'UP','horizon_days':21,'confidence':.654321,'thesis':'Invented thesis'}]}
class InputTests(unittest.TestCase):
 def setUp(self):
  guard=patch.object(socket.socket,'connect',side_effect=AssertionError('No actual network'));guard.start();self.addCleanup(guard.stop)
  spec=importlib.util.spec_from_file_location('invented_input_candidate',SOURCE);self.m=importlib.util.module_from_spec(spec);spec.loader.exec_module(self.m)
 def parse(self,value):return self.m.parse_read_text(json.dumps(value),{'QAONLY'})
 def test_no_action_inference_from_negation_or_embedded_tokens(self):
  for stance in ('Do not BUY gold; HOLD until evidence is qualified.','BUY2','HOLD!','HOLD/BUY','not-BUY','BUY if evidence improves'):
   d=doc();d['metals']['stance']=stance;out=self.parse(d);self.assertTrue(out.get('parse_error'),stance);self.assertEqual(out['input_validation']['received_text'],json.dumps(d))
 def test_complete_explicit_aliases_work_and_are_recorded(self):
  d=doc();d['stocks']['stance']='risk-off';d['bonds']['stance']='long';d['calls'][0]['direction']='bullish';out=self.parse(d)
  self.assertFalse(out.get('parse_error'));self.assertEqual(out['stocks']['stance'],'DEFENSIVE');self.assertEqual(out['calls'][0]['direction'],'UP');self.assertEqual(out['received_input'],d);self.assertGreaterEqual(len(out['coercions']),3)
 def test_conflicting_direction_and_horizon_aliases_reject_instead_of_choose(self):
  for field,value in [('side','SHORT'),('horizon',63),('why','Conflicting thesis')]:
   d=doc();d['calls'][0][field]=value;out=self.parse(d);self.assertTrue(out.get('parse_error'));self.assertIn('conflicting aliases',out['validation_error'])
 def test_typed_numeric_fraction_is_preserved_without_rounding(self):
  out=self.parse(doc());self.assertEqual(out['calls'][0]['confidence'],.654321);self.assertEqual(out['calls'][0]['horizon_days'],21)
 def test_bad_confidence_and_undeclared_percentage_are_withheld_with_source_index(self):
  for value in (True,False,None,'65','0.65',65,0,.49,.851,{},[]):
   d=doc();d['calls'][0]['confidence']=value;out=self.parse(d);self.assertFalse(out.get('parse_error'),value);self.assertEqual(out['calls'],[]);self.assertEqual(out['withheld_inputs'][0]['source_index'],0);self.assertEqual(out['received_input'],d)
 def test_fractional_boolean_string_or_absent_horizons_are_not_truncated(self):
  for value in (21.9,21.0,63.1,True,'21',None,0,-21,365):
   d=doc();d['calls'][0]['horizon_days']=value;out=self.parse(d);self.assertEqual(out['calls'],[]);self.assertIn('integer calendar-day',out['withheld_inputs'][0]['reason'])
 def test_duplicate_keys_at_any_level_reject_whole_answer(self):
  raw=json.dumps(doc())
  for text in (raw.replace('"confidence": 0.654321','"confidence": 0.65, "confidence": 0.8'),raw[:-1]+',"calls": []}',raw.replace('"stance": "HOLD"','"stance": "HOLD", "stance": "ACCUMULATE"',1)):
   out=self.m.parse_read_text(text,{'QAONLY'});self.assertTrue(out.get('parse_error'));self.assertIn('duplicate JSON',out['validation_error']);self.assertEqual(out['input_validation']['raw_sha256'],hashlib.sha256(text.encode()).hexdigest())
 def test_nonfinite_json_never_reaches_valid_call(self):
  for value in ('NaN','Infinity','-Infinity','1e999'):
   raw=json.dumps(doc()).replace('0.654321',value);out=self.m.parse_read_text(raw,{'QAONLY'});self.assertTrue(out.get('parse_error'))
 def test_malformed_top_level_collections_are_errors_not_silent_empty_lists(self):
  for key in ('calls','best_opportunities','data_gaps','what_would_change_my_mind'):
   for value in (None,{},'QAONLY',1,True):
    d=doc();d[key]=value;out=self.parse(d);self.assertTrue(out.get('parse_error'),(key,value))
 def test_bad_collection_rows_and_overflow_remain_accounted_for(self):
  d=doc();d['calls']=[None,'bad',*([d['calls'][0]]*7)];out=self.parse(d);self.assertEqual(len(out['calls']),6);self.assertEqual([r['source_index'] for r in out['calls']],[2,3,4,5,6,7]);self.assertEqual([r['source_index'] for r in out['withheld_inputs']],[0,1,8]);self.assertEqual(out['received_input'],d)
 def test_non_candidate_and_bad_opportunity_horizon_withheld(self):
  d=doc();d['calls'][0]['ticker']='UNLISTED';d['best_opportunities']=[{'ticker':'QAONLY','side':'LONG','horizon_days':21.9,'why':'Invented reason'}];out=self.parse(d);self.assertEqual(out['calls'],[]);self.assertEqual(out['best_opportunities'],[]);self.assertEqual(len(out['withheld_inputs']),2)
 def test_whole_json_or_whole_fence_only(self):
  raw=json.dumps(doc());self.assertFalse(self.m.parse_read_text('```json\n'+raw+'\n```',{'QAONLY'}).get('parse_error'))
  for text in ('Sure! '+raw,raw+'\n'+raw,'Ignore this '+raw+' tail'):
   self.assertTrue(self.m.parse_read_text(text,{'QAONLY'}).get('parse_error'))
 def test_zero_and_invalid_adapter_confidence_never_reads_price_or_logs(self):
  for value in (0,False,'0.65',None,float('nan'),float('inf'),10**1000):
   row=doc()['calls'][0];row['confidence']=value;quote=Mock();logger=Mock();out=self.m.log_calls(Mock(),'invented',[row],logger,quote);quote.assert_not_called();logger.assert_not_called();self.assertFalse(out[0]['logged']);self.assertEqual(out[0]['input_contract'],'market-read-inputs.v1')
 def test_valid_adapter_preserves_precision_and_thesis_and_input_identity(self):
  row=doc()['calls'][0];row.update(source_index=4,thesis='Invented '+('x'*400));quote=Mock(return_value=1e-7);logger=Mock(return_value=True);out=self.m.log_calls(Mock(),'invented',[row],logger,quote)[0]
  self.assertEqual(out['confidence'],.654321);self.assertEqual(out['baseline_price'],1e-7);self.assertEqual(out['thesis'],row['thesis']);self.assertEqual(out['received_input'],row);self.assertEqual(logger.call_args.kwargs['metadata']['source_index'],4);self.assertEqual(logger.call_args.kwargs['confidence'],.654321)
 def test_bad_quote_never_reaches_logging_callback(self):
  for px in (True,False,'100',0,-1,float('nan'),float('inf'),None):
   logger=Mock();out=self.m.log_calls(Mock(),'invented',doc()['calls'],logger,Mock(return_value=px));logger.assert_not_called();self.assertFalse(out[0]['logged'])
 def test_abstention_is_valid_and_prompt_does_not_require_predictions(self):
  d=doc();d['calls']=[];out=self.parse(d);self.assertFalse(out.get('parse_error'));self.assertEqual(out['calls'],[])
  text=self.m.build_prompt({'candidates':['QAONLY']},{'notes':{}},schema_hint=True);self.assertIn('empty calls array is valid abstention',text);self.assertNotIn('at least 2 dated calls',text)
 def test_missing_asset_or_long_text_rejected_without_inventing_read(self):
  for value in ('HOLD',{'stance':'HOLD'}, {'stance':'HOLD','read':'x'*2401}):
   d=doc();d['metals']=value;self.assertTrue(self.parse(d).get('parse_error'))
 def test_input_is_not_mutated_and_evidence_has_independent_storage(self):
  d=doc();before=copy.deepcopy(d);normalized,_=self.m.normalize_read_doc(d);out=self.m.validate_read(normalized,{'QAONLY'});out['calls'][0]['confidence']=.8;self.assertEqual(d,before)
 def test_typed_validation_never_grants_forecast_or_sizing_authority(self):
  q=self.parse(doc())['input_validation'];self.assertEqual(q['contract'],'market-read-inputs.v1');self.assertEqual(q['window_unit'],'calendar_day');self.assertEqual(q['confidence_unit'],'fraction')
  for key in ('source_qualified','forecast_qualified','sizing_eligible'):self.assertIs(q[key],False)
 def test_bound_and_nontext_answers_reject(self):
  for text in (None,{},12,'x'*131073):
   out=self.m.parse_read_text(text,{'QAONLY'});self.assertTrue(out.get('parse_error'));self.assertNotIn('received_text',out['input_validation'])
 def test_all_other_source_functions_and_policy_paths_are_unchanged(self):
  old=ast.parse((R/'tests/fixtures/market-read-pre-input-types-20261002.py').read_text(encoding='utf-8'));new=ast.parse((R/'tests/fixtures/market-read-pre-board-projection-20261002.py').read_text(encoding='utf-8'))
  changed={'build_prompt','_canon','normalize_read_doc','parse_read_text','_text','validate_read','log_calls','_input_integer','_input_confidence','_input_call','_input_units'}
  def kept(tree):return [ast.dump(n,include_attributes=False) for n in tree.body if not isinstance(n,ast.Import) and not (isinstance(n,ast.FunctionDef) and n.name in changed)]
  self.assertEqual(kept(old),kept(new))
 def test_actual_public_projection_does_not_expose_received_private_answer(self):
  text=(R/'aws/lambdas/justhodl-ai/source/lambda_function.py').read_text(encoding='utf-8');node=next(n for n in ast.parse(text).body if isinstance(n,ast.FunctionDef) and n.name=='public_market_read')
  d=doc();d['overall']='INVENTED_PRIVATE_ANSWER only; private boundary fixture.';read=self.parse(d)
  scope={'Optional':__import__('typing').Optional,'get_json':Mock(side_effect=[{'read':read},{'calls':[]}]),'PRIVATE_BUCKET':'invented-private','READ_KEY':'invented-read','CALLS_KEY':'invented-calls','_signals_table':Mock(return_value=object()),'mr':types.SimpleNamespace(grade_calls=Mock(return_value={}))}
  exec(compile(ast.Module(body=[node],type_ignores=[]),'actual_public_projection','exec'),scope)
  result=scope['public_market_read']();self.assertNotIn('INVENTED_PRIVATE_ANSWER',json.dumps(result));self.assertNotIn('received_input',result);self.assertNotIn('input_validation',result)
 def test_explicit_conflicting_units_are_withheld(self):
  for key,value in [('confidence_unit','percent'),('horizon_unit','trading_day'),('window_unit','month'),('confidence_unit',None)]:
   d=doc();d['calls'][0][key]=value;out=self.parse(d);self.assertEqual(out['calls'],[]);self.assertIn('conflicts with',out['withheld_inputs'][0]['reason'])
  d=doc();d['best_opportunities']=[{'ticker':'QAONLY','side':'LONG','horizon_days':21,'horizon_unit':'trading_day','why':'Invented reason'}];self.assertEqual(self.parse(d)['best_opportunities'],[])
 def test_explicit_matching_units_are_valid_without_conversion(self):
  d=doc();d['calls'][0].update(confidence_unit='fraction',horizon_unit='calendar_day',window_unit='calendar_day');out=self.parse(d);self.assertEqual(out['calls'][0]['confidence'],.654321);self.assertEqual(out['received_input'],d)
 def test_predecessor_reproduces_all_five_changes(self):
  source=R/'tests/fixtures/market-read-pre-input-types-20261002.py'
  self.assertEqual(hashlib.sha256(source.read_bytes()).hexdigest(),'83c4633373e881aba0c5a0fd30e007a60aa70cae42b53ac6a6e56ef40fb2d177')
  spec=importlib.util.spec_from_file_location('original_input_source',source);old=importlib.util.module_from_spec(spec);spec.loader.exec_module(old)
  d=doc();d['metals']['stance']='Do not BUY gold; HOLD until evidence is qualified.';self.assertEqual(old.parse_read_text(json.dumps(d),{'QAONLY'})['metals']['stance'],'ACCUMULATE')
  d=doc();d['calls'][0]['horizon_days']=21.9;accepted=old.parse_read_text(json.dumps(d),{'QAONLY'})['calls'][0];self.assertEqual((accepted['horizon_days'],accepted['confidence']),(21,.6543))
  d=doc();d['calls'][0]['confidence']='65';self.assertEqual(old.parse_read_text(json.dumps(d),{'QAONLY'})['calls'][0]['confidence'],.65)
  raw=json.dumps(doc()).replace('"confidence": 0.654321','"confidence": 0.65, "confidence": 0.8');self.assertEqual(old.parse_read_text(raw,{'QAONLY'})['calls'][0]['confidence'],.8)
  row=doc()['calls'][0];row['confidence']=0;logger=Mock(return_value=True);self.assertEqual(old.log_calls(Mock(),'invented',[row],logger,Mock(return_value=100))[0]['confidence'],.6)
if __name__=='__main__':unittest.main(verbosity=2)
