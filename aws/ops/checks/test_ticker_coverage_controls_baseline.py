from pathlib import Path
import copy,runpy,sys,types,unittest
from unittest.mock import Mock,patch
R=Path(__file__).resolve().parents[3];FN='justhodl-contract-gate'
CFG={'FunctionName':FN,'FunctionArn':'invented-arn','CodeSha256':'invented-code-hash','State':'Active','LastUpdateStatus':'Successful','Runtime':'python3.12','Handler':'lambda_function.lambda_handler','Timeout':180,'MemorySize':512,'Architectures':['x86_64'],'Role':'invented-role','EphemeralStorage':{'Size':512},'Environment':{'Variables':{'PRIVATE_CANARY':'DO_NOT_RETURN'}}}
class Baseline(unittest.TestCase):
 def load(self):
  helper=types.SimpleNamespace(schedule_evidence=Mock(return_value=[{'kind':'EventBridge rule','name':'invented-hourly','state':'ENABLED','expression':'cron(25 * * * ? *)','native_targets':1}]),verified_alias=Mock(return_value=None))
  with patch.dict(sys.modules,{'market_runtime_evidence':helper}):m=runpy.run_path(str(R/'aws/ops/staged/ops_6418_ticker_coverage_controls_baseline.py'))
  m['capture'].__globals__['ROOT']=R;return m,helper
 def test_selected_controls_only_and_missing_local_config_supported(self):
  m,h=self.load();lam=Mock();lam.get_function_configuration.return_value=copy.deepcopy(CFG);out=m['capture'](lam,Mock(),Mock(),FN)
  self.assertNotIn('Environment',out);self.assertNotIn('PRIVATE_CANARY',repr(out));self.assertEqual(out['Timeout'],180);self.assertEqual(len(out['schedules']),1);lam.get_function_configuration.assert_called_once_with(FunctionName=FN)
 def test_wrong_identity_or_unready_function_is_not_baseline(self):
  for key,value in [('FunctionName','other'),('State','Inactive'),('LastUpdateStatus','InProgress')]:
   m,h=self.load();lam=Mock();lam.get_function_configuration.return_value={**CFG,key:value}
   with self.subTest(key=key),self.assertRaises(ValueError):m['capture'](lam,Mock(),Mock(),FN)
   h.schedule_evidence.assert_not_called()
 def test_missing_invalid_and_boolean_counts_are_rejected(self):
  for key,value in [('Timeout',True),('Timeout',0),('MemorySize',None),('EphemeralStorage',{'Size':False}),('Architectures',[]),('Role','')]:
   m,h=self.load();lam=Mock();lam.get_function_configuration.return_value={**CFG,key:value}
   with self.subTest(key=key),self.assertRaises(ValueError):m['capture'](lam,Mock(),Mock(),FN)
 def test_absent_schedule_not_silently_accepted_as_original_cadence(self):
  m,h=self.load();h.schedule_evidence.return_value=[];lam=Mock();lam.get_function_configuration.return_value=copy.deepcopy(CFG)
  with self.assertRaises(ValueError):m['capture'](lam,Mock(),Mock(),FN)
 def test_denied_control_read_propagates(self):
  m,h=self.load();lam=Mock();lam.get_function_configuration.side_effect=PermissionError('invented denial')
  with self.assertRaises(PermissionError):m['capture'](lam,Mock(),Mock(),FN)
 def test_schedule_inventory_failure_is_not_absence(self):
  m,h=self.load();h.schedule_evidence.side_effect=PermissionError('invented denial');lam=Mock();lam.get_function_configuration.return_value=copy.deepcopy(CFG)
  with self.assertRaises(PermissionError):m['capture'](lam,Mock(),Mock(),FN)
 def main_fixture(self,changed=False):
  m,h=self.load();out=Mock();manager=Mock();manager.__enter__=Mock(return_value=out);manager.__exit__=Mock(return_value=False)
  calls=[]
  def capture(lam,events,scheduler,fn):
   calls.append(fn);return {'function':fn,'control':2 if changed and len(calls)>len(m['FUNCTIONS']) else 1}
  m['main'].__globals__['capture']=capture
  external={'boto3':types.SimpleNamespace(client=Mock(side_effect=lambda *a,**kw:object())), 'ops_report':types.SimpleNamespace(report=Mock(return_value=manager))}
  return m,out,calls,external
 def test_main_captures_every_named_function_twice_without_invoking(self):
  m,out,calls,external=self.main_fixture()
  with patch.dict(sys.modules,external):m['main']()
  self.assertEqual(calls,list(m['FUNCTIONS'])*2);self.assertEqual(len(m['FUNCTIONS']),3)
  evidence=out.kv.call_args.kwargs['evidence'];self.assertFalse(evidence['normal_publication_verified']);self.assertFalse(evidence['investment_authority'])
  for key in ['native_invocations','provider_requests','application_packet_reads','private_reads','account_reads','native_writes','schedule_changes']:self.assertEqual(evidence[key],0)
  self.assertEqual([c.args[0] for c in external['boto3'].client.call_args_list],['lambda','events','scheduler'])
 def test_changed_controls_prevent_success_evidence(self):
  m,out,calls,external=self.main_fixture(changed=True)
  with patch.dict(sys.modules,external),self.assertRaises(ValueError):m['main']()
  out.kv.assert_not_called()
if __name__=='__main__':unittest.main(verbosity=2)
