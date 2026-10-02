from pathlib import Path
from unittest.mock import Mock,patch
from decimal import Decimal
import importlib.util,socket,sys,unittest
R=Path(__file__).resolve().parents[4]
sys.path[:0]=[str(R/'aws/lambdas/justhodl-ai/source'),str(R/'aws/shared')]
class Tests(unittest.TestCase):
 def setUp(self):
  self.guard=patch.object(socket.socket,'connect',side_effect=AssertionError('Invented data only'));self.guard.start();self.addCleanup(self.guard.stop)
  spec=importlib.util.spec_from_file_location('invented_mr_projection',R/'aws/lambdas/justhodl-ai/source/market_read.py');self.m=importlib.util.module_from_spec(spec);spec.loader.exec_module(self.m)
  self.call={'ticker':'QAONLY','direction':'UP','horizon_days':21,'confidence':.6,'baseline_price':100,'signal_id':'invented#QAONLY','logged':True,'thesis':'Invented'}
 def project(self,outcome):
  t=Mock();t.get_item.return_value={'Item':{'status':'partial','outcomes':{'day_21':outcome}}};return self.m.grade_calls(t,[self.call])
 def test_explicit_false_is_a_miss_not_missing(self):
  out=self.project({'return_pct':-5,'correct':False});self.assertEqual(out['by_window']['21'],{'hits':0,'n':1,'hit_rate':0.0});self.assertFalse(out['rows'][0]['windows']['21']['correct'])
 def test_zero_is_an_explicit_reported_value(self):
  out=self.project({'return_pct':0,'correct':True});self.assertEqual(out['rows'][0]['windows']['21']['return_pct'],0.0);self.assertEqual(out['by_window']['21']['n'],1)
 def test_missing_grade_is_not_recalculated(self):
  for val in (None,'false','true',0,1,{},[]):
   out=self.project({'return_pct':1,'correct':val});self.assertEqual(out['by_window']['21']['n'],0);self.assertEqual(out['rows'][0]['windows'],{});self.assertIn('21',out['rows'][0]['withheld_windows'])
 def test_invalid_numeric_return_never_enters_summary(self):
  for val in (True,False,'1.2',None,float('nan'),float('inf'),Decimal('NaN'),Decimal('Infinity')):
   out=self.project({'return_pct':val,'correct':True});self.assertEqual(out['by_window']['21']['n'],0);self.assertIsNone(out['rows'][0]['withheld_windows']['21']['return_pct'])
 def test_explicit_failure_status_cannot_be_a_hit(self):
  for status in ('UNSCOREABLE','PENDING','partial','UNKNOWN',True):
   out=self.project({'return_pct':1,'correct':True,'status':status});self.assertEqual(out['by_window']['21']['n'],0)
 def test_legacy_price_without_return_cannot_fabricate_performance(self):
  out=self.project({'price':101,'correct':True});self.assertEqual(out['by_window']['21']['n'],0);self.assertIsNone(out['rows'][0]['withheld_windows']['21']['return_pct'])
 def test_exact_received_return_not_rounded_to_four_places(self):
  out=self.project({'return_pct':Decimal('0.123456789'),'correct':True});self.assertEqual(out['rows'][0]['windows']['21']['return_pct'],.123456789)
 def test_withheld_windows_do_not_reach_either_lesson_path(self):
  out=self.project({'return_pct':1,'correct':'false'});complete=Mock(side_effect=AssertionError('No model calls'));fallback=Mock(side_effect=AssertionError('No model calls'));self.m.write_lessons(out['rows'],{},complete,fallback)
  complete.assert_not_called();fallback.assert_not_called()
  import gear_b_doctrine as gd
  with patch.dict(sys.modules,{'market_read':self.m}):
   gd._wrap_market_read();self.m.write_lessons(out['rows'],{},complete,fallback)
  complete.assert_not_called();fallback.assert_not_called()
 def test_no_qualification_from_valid_typed_report(self):
  out=self.project({'return_pct':1,'correct':True});q=out['qualification'];self.assertEqual(q['window_unit'],'calendar_day')
  for k in ('forecast_qualified','out_of_sample_verified','cost_adjusted','sizing_eligible'):self.assertIs(q[k],False)
 def test_original_false_missing_and_failed_reports_reproduce_false_wins(self):
  import hashlib
  p=R/'tests/fixtures/market-read-pre-outcome-types-20261002.py'
  self.assertEqual(hashlib.sha256(p.read_bytes()).hexdigest(),'1fdd770b7a0a2d4f13abcc0caa04f5ea479fee1171d5567dc0e6e8421f7c1ca6')
  spec=importlib.util.spec_from_file_location('original_market_read',p);old=importlib.util.module_from_spec(spec);spec.loader.exec_module(old)
  for outcome in ({'return_pct':-5,'correct':'false'},{'return_pct':1,'correct':None},{'return_pct':True,'correct':None},{'return_pct':1,'correct':True,'status':'UNSCOREABLE'}):
   table=Mock();table.get_item.return_value={'Item':{'status':'partial','outcomes':{'day_21':outcome}}}
   self.assertEqual(old.grade_calls(table,[self.call])['by_window']['21'],{'hits':1,'n':1,'hit_rate':1.0})
   self.assertEqual(self.m.grade_calls(table,[self.call])['by_window']['21'],{'hits':0,'n':0,'hit_rate':None})
 def test_actual_student_desk_does_not_promote_window_counts_to_market_skill(self):
  import ast,hashlib
  old=R/'tests/fixtures/ai-student-desk-pre-outcomes-20261002.py'
  self.assertEqual(hashlib.sha256(old.read_bytes()).hexdigest(),'8fdf9312347d67611f7225b6a86608c0549dfbb330164ba6d8c466dea3281956')
  def desk(path):
   tree=ast.parse(path.read_text(encoding='utf-8'));node=next(n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name=='student_desk');ns={'now_iso':lambda:'2026-10-02T12:00:00Z'}
   exec(compile(ast.fix_missing_locations(ast.Module(body=[node],type_ignores=[])),str(path),'exec'),ns);return ns['student_desk']
  before=desk(old);after=desk(R/'aws/lambdas/justhodl-ai/source/lambda_function.py')
  for count in (0,19,20,25,240):
   pub={'scoreboard':{'calls_made':11,'calls_graded':count,'reported_outcome_qualification':{'forecast_qualified':True}},'market_exam':{'holdout':{'model_scores':{'score':.99},'baselines':{'prior':{'score':.5}}}}}
   if count>=20:self.assertIn('Market skill exists',before(pub)['next_lesson'])
   out=after(pub);self.assertEqual(out['calls']['graded'],count);self.assertEqual(out['calls']['graded_unit'],'reported_window');self.assertIs(out['calls']['qualification']['forecast_qualified'],False);self.assertNotIn('Market skill exists',out['next_lesson']);self.assertIn('do not grant promotion',out['next_lesson'])
def load_tests(loader, tests, pattern):
 import test_market_read_inputs
 tests.addTests(loader.loadTestsFromModule(test_market_read_inputs))
 import test_market_read_board
 tests.addTests(loader.loadTestsFromModule(test_market_read_board))
 import test_market_read_arithmetic
 tests.addTests(loader.loadTestsFromModule(test_market_read_arithmetic))
 spec=importlib.util.spec_from_file_location('actual_desk_input_tests',R/'aws/shared/tests/test_deterministic_desk_inputs.py')
 module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
 tests.addTests(loader.loadTestsFromModule(module))
 return tests
if __name__=='__main__':unittest.main(verbosity=2)
