from pathlib import Path
from unittest.mock import patch
from decimal import Decimal
import ast,hashlib,importlib.util,json,socket,sys,unittest
R=Path(__file__).resolve().parents[3]
class Tests(unittest.TestCase):
 def setUp(self):
  guard=patch.object(socket.socket,'connect',side_effect=AssertionError('Invented data only'));guard.start();self.addCleanup(guard.stop)
  spec=importlib.util.spec_from_file_location('invented_desk',R/'aws/shared/deterministic_desk.py');self.m=importlib.util.module_from_spec(spec);spec.loader.exec_module(self.m)
 def test_reproduced_cases_cannot_grant_permission(self):
  cases=json.loads((R/'docs/audit/2026-10-02/deterministic-desk-reproductions.json').read_bytes())['findings']
  for case in cases:
   self.assertIs(self.m.execute_task('veto-check',case['received'])['allowed'],False,case['case'])
 def test_all_explicit_zero_constraints_veto_every_directional_arm(self):
  for board in ({'regime':{'risk_gate_posture':'RISK_ON','risk_gate_sizing':0},'sizing_multiplier':1},{'regime':{'risk_gate_posture':'RISK_ON'},'authority':{'cap':0,'sizing_multiplier':1}},{'regime':{'risk_gate_posture':'RISK_ON','authority_cap_pct':0},'authority':{'cap':1}},{'regime':{'risk_gate_posture':'RISK_ON','authority_allows_new_entries':False}}):
   out=self.m.desk_read(board);self.assertFalse(out['ok']);self.assertEqual({out[k]['stance'] for k in ('stocks','bonds','metals','crypto')},{'NO_READ'});self.assertTrue(out['vetoes'])
 def test_complete_explicit_posture_only(self):
  for val in ('Do not use RISK_ON','NO_RISK_ON','OFFICE','NEUTRAL2',True,{},None):self.assertIsNone(self.m._gate(val))
  self.assertEqual(self.m._gate('risk_on'),'RISK_ON')
 def test_numeric_parser_rejects_boolean_text_and_nonfinite(self):
  for val in (True,False,'0','100',None,float('inf'),float('nan'),Decimal('NaN'),Decimal('1e-999'),10**1000):self.assertIsNone(self.m._num(val))
  self.assertEqual(self.m._num(0),0);self.assertEqual(self.m._num(Decimal('0.123456789')),.123456789)
 def test_missing_legs_are_unknown_instead_of_passing_checks(self):
  out=self.m.desk_read({'regime':{'risk_gate_posture':'RISK_ON'}})
  self.assertEqual(set(out['data_gaps']),{'authority','sizing','plumbing','credit','dollar','vol','constitution'})
  for row in out['reasoning']:
   if row['step'] in out['data_gaps']:self.assertIsNone(row['ok'])
 def test_no_earned_authority_even_when_all_reported_constraints_clear(self):
  b={'regime':{'risk_gate_posture':'RISK_ON','risk_gate_sizing':1,'funding_score':0,'dxy_63d_pct':0,'vix':10},'authority':{'allows_new_entries':True,'cap':1},'constitution_posture':'balanced','legs':{'credit':{'score':0,'ccc_21d_pct':0}}}
  out=self.m.execute_task('veto-check',b);self.assertFalse(out['allowed']);self.assertTrue(out['reported_rule_result']);self.assertEqual(out['data_gaps'],[])
  for key in ('source_qualified','forecast_qualified','sizing_eligible','execution_eligible'):self.assertIs(out['qualification'][key],False)
 def test_canonical_zero_dollar_change_not_replaced_by_legacy_stress(self):
  out=self.m.desk_read({'regime':{'risk_gate_posture':'RISK_ON','dxy_63d_pct':0},'legs':{'dollar':{'dxy_63d_pct':5}}})
  row=next(x for x in out['reasoning'] if x['step']=='dollar');self.assertTrue(row['ok']);self.assertEqual(row['why'],0)
 def test_malformed_and_missing_board_fails_closed(self):
  for b in (None,{},'RISK_ON',[],{'regime':[]},{'regime':{'risk_gate_posture':'not RISK_ON'}}):
   out=self.m.desk_read(b);self.assertFalse(out['ok']);self.assertEqual(out['stocks']['stance'],'NO_READ')
 def test_asset_no_read_records_do_not_share_mutable_state(self):
  out=self.m.desk_read({});out['stocks']['read']='changed';self.assertNotEqual(out['bonds']['read'],'changed')
 def test_invalid_explicit_cap_does_not_borrow_valid_secondary_cap(self):
  for value in ('0',True,None,float('nan')):
   out=self.m.desk_read({'regime':{'risk_gate_posture':'RISK_ON'},'authority':{'allows_new_entries':True,'cap':value,'sizing_multiplier':1}})
   row=next(r for r in out['reasoning'] if r['step']=='authority');self.assertIsNone(row['ok'])
 def test_original_five_permission_errors_remain_reproducible(self):
  p=R/'tests/fixtures/deterministic-desk-pre-input-types-20261002.py';repros=json.loads((R/'docs/audit/2026-10-02/deterministic-desk-reproductions.json').read_bytes());self.assertEqual(hashlib.sha256(p.read_bytes()).hexdigest(),repros['source_sha256'])
  spec=importlib.util.spec_from_file_location('original_desk',p);old=importlib.util.module_from_spec(spec);spec.loader.exec_module(old)
  for case in repros['findings']:
   self.assertEqual(old.execute_task('veto-check',case['received']),case['result']);self.assertTrue(case['result']['allowed'])
 def test_existing_rule_table_and_thresholds_are_descriptive_and_unchanged(self):
  p=R/'tests/fixtures/deterministic-desk-pre-input-types-20261002.py';spec=importlib.util.spec_from_file_location('original_rule_table',p);old=importlib.util.module_from_spec(spec);spec.loader.exec_module(old);self.assertEqual(self.m.TABLE,old.TABLE);self.assertEqual(self.m.RANK,old.RANK)
  for gate,const in old.TABLE:
   for funding,credit,ccc,dxy,vix in [(0,0,0,0,10),(-1.5,-1,8,3,25),(-1.49,-.99,7.99,2.99,24.99)]:
    b={'regime':{'risk_gate_posture':gate,'risk_gate_sizing':1,'funding_score':funding,'dxy_63d_pct':dxy,'vix':vix},'authority':{'allows_new_entries':True,'cap':1},'constitution_posture':const,'legs':{'credit':{'score':credit,'ccc_21d_pct':ccc}}}
    self.assertEqual(self.m.think(b)[0],old.think(b)[0]);self.assertFalse(self.m.execute_task('veto-check',b)['allowed'])
 def test_actual_ai_fallback_uses_repaired_constraint_path(self):
  src=R/'aws/lambdas/justhodl-ai/source/market_read.py';sys.path[:0]=[str(src.parent),str(R/'aws/shared')]
  with patch.dict(sys.modules,{'deterministic_desk':self.m}):
   spec=importlib.util.spec_from_file_location('actual_fallback',src);m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)
  out=m.deterministic_read({'regime':{'risk_gate_posture':'RISK_ON','risk_gate_sizing':0},'sizing_multiplier':1})
  self.assertEqual(out['stocks']['stance'],'NO_READ');self.assertFalse(out['qualification']['sizing_eligible']);self.assertEqual(out['calls'],[])
if __name__=='__main__':unittest.main(verbosity=2)
