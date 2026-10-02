from pathlib import Path
from unittest.mock import Mock,patch
import copy,runpy,sys,types,unittest
R=Path(__file__).resolve().parents[3]
FN='justhodl-alpha-research';COMMIT='a'*40
CONTROL={'function_name':FN,'timeout':120,'memory_mb':256,'runtime':'python3.12','handler':'lambda_function.lambda_handler','architectures':['x86_64'],'role':'invented-role','ephemeral_storage_mb':512,'schedules':[{'kind':'EventBridge rule','name':'invented-daily','state':'ENABLED','expression':'cron(0 21 ? * MON-FRI *)','native_targets':1}]}
class Acceptance(unittest.TestCase):
 def load(self):
  helper=types.ModuleType('market_runtime_evidence');helper.BUCKET='invented';helper.runtime=Mock();helper.schedule_evidence=Mock();helper.verified_alias=Mock()
  with patch.dict(sys.modules,{'market_runtime_evidence':helper}):m=runpy.run_path(str(R/'aws/ops/staged/ops_6447_heartbeat_acceptance.py'))
  g=m['normalize'].__globals__;g['EXPECTED_CONTROLS']={FN:copy.deepcopy(CONTROL)};g['EXPECTED_SOURCE_COUNTS']={FN:3};g['ScheduleInventory']=lambda value:value;self.helper=helper;return m
 def value(self):return dict(copy.deepcopy(CONTROL),source_files_checked=3,handler_bytes=123,code_sha256='invented',receipt={'status':'matched','commit':COMMIT})
 def test_exact_controls_and_whole_sources_accepted(self):
  m=self.load();value=self.value();self.assertEqual(m['normalize'](value,FN,COMMIT),value)
 def test_complete_rule_and_scheduler_controls_compare_independently_of_order(self):
  m=self.load();value=self.value();scheduler={'kind':'EventBridge Scheduler','name':'invented-sched','state':'ENABLED','expression':'cron(0 21 ? * MON-FRI *)','native_targets':1,'timezone':'UTC','group':'default'}
  expected=m['normalize'].__globals__['EXPECTED_CONTROLS'][FN]
  expected['schedules'].append(scheduler);value['schedules'].append(copy.deepcopy(scheduler))
  for observed in [list(value['schedules']),list(reversed(value['schedules']))]:
   result=m['normalize']({**value,'schedules':observed},FN,COMMIT)
   self.assertEqual(result['schedules'],expected['schedules'])
  for observed in [value['schedules'][:1],value['schedules']*2,[{**scheduler,'state':'DISABLED'},value['schedules'][0]]]:
   with self.assertRaises(ValueError):m['normalize']({**value,'schedules':observed},FN,COMMIT)
 def test_wrong_function_receipt_or_missing_sources_rejected(self):
  for key,value in [('function_name','other'),('receipt',{'status':'matched','commit':'b'*40}),('source_files_checked',2),('source_files_checked',True)]:
   m=self.load();v=self.value();v[key]=value
   with self.subTest(key=key),self.assertRaises(ValueError):m['normalize'](v,FN,COMMIT)
 def test_schedule_cadence_state_or_resources_cannot_drift(self):
  for key,value in [('timeout',121),('memory_mb',512),('role','other'),('architectures',['arm64']),('schedules',[])]:
   m=self.load();v=self.value();v[key]=value
   with self.subTest(key=key),self.assertRaises(ValueError):m['normalize'](v,FN,COMMIT)
  m=self.load();v=self.value();v['schedules'][0]['state']='DISABLED'
  with self.assertRaises(ValueError):m['normalize'](v,FN,COMMIT)
 def test_native_change_between_two_whole_captures_fails(self):
  m=self.load();v=self.value();other=dict(v,code_sha256='changed');self.helper.runtime.side_effect=[v,other]
  with self.assertRaises(ValueError):m['inspect']((object(),)*4,COMMIT)
 def test_native_read_failure_propagates(self):
  m=self.load();self.helper.runtime.side_effect=PermissionError('invented denial')
  with self.assertRaisesRegex(ValueError,'Named release acceptance failed:.* PermissionError'):m['inspect']((object(),)*4,COMMIT)
 def test_empty_binding_census_is_accepted_only_when_baseline_is_empty(self):
  m=self.load();v=self.value();v['schedules']=[]
  with self.assertRaises(ValueError):m['normalize'](v,FN,COMMIT)
  m['normalize'].__globals__['EXPECTED_CONTROLS'][FN]['schedules']=[]
  self.assertEqual(m['normalize'](v,FN,COMMIT)['schedules'],[])
  for invalid in (None,{},False,[None]):
   v['schedules']=invalid
   with self.assertRaises(ValueError):m['normalize'](v,FN,COMMIT)
 def test_two_snapshots_build_two_separate_complete_censuses(self):
  m=self.load();v=self.value();self.helper.runtime.side_effect=[v,v]
  inventory=Mock(side_effect=lambda value:object());m['inspect'].__globals__['ScheduleInventory']=inventory
  before,after=m['inspect']((object(),)*4,COMMIT);self.assertEqual(before,after);self.assertEqual(inventory.call_count,2)
 def test_signed_url_in_native_error_never_reaches_report(self):
  m=self.load();self.helper.runtime.side_effect=RuntimeError('https://example.invalid/?signature=INVENTED_PRIVATE_VALUE')
  with self.assertRaises(ValueError) as e:m['inspect']((object(),)*4,COMMIT)
  self.assertNotIn('signature',str(e.exception));self.assertNotIn('INVENTED_PRIVATE_VALUE',str(e.exception));self.assertIn('RuntimeError',str(e.exception))
 def test_only_named_public_receipt_can_be_read(self):
  m=self.load();client=Mock();guard=m['ReceiptOnly'](client)
  guard.get_object(Bucket='invented',Key='data/ops/releases/'+FN+'.json');self.assertEqual(client.get_object.call_count,1)
  for key in ['data/bond-trace.json','portfolio/account.json','data/ops/releases/other.json']:
   with self.subTest(key=key),self.assertRaises(ValueError):guard.get_object(Bucket='invented',Key=key)
  self.assertEqual(client.get_object.call_count,1)
class Targets(unittest.TestCase):
 def load(self):
  m=Acceptance().load();m['verify_binding_identities'].__globals__['MONITORED_TARGETS']={FN:'invented-arn'};return m
 def test_real_named_target_identity_is_read_without_returning_environment(self):
  m=self.load();lam=Mock();lam.get_function_configuration.return_value={'FunctionName':FN,'FunctionArn':'invented-arn','State':'Active','Environment':{'Variables':{'PRIVATE':'DO_NOT_RETURN'}}};result=m['verify_binding_identities'](lam);self.assertEqual(result,{FN:'invented-arn'});self.assertNotIn('PRIVATE',repr(result));lam.get_function_configuration.assert_called_once_with(FunctionName=FN)
 def test_wrong_function_target_or_unready_identity_is_rejected(self):
  for change in [{'FunctionName':'other'},{'FunctionArn':'other'},{'State':'Inactive'}]:
   m=self.load();lam=Mock();lam.get_function_configuration.return_value={'FunctionName':FN,'FunctionArn':'invented-arn','State':'Active',**change}
   with self.assertRaises(ValueError):m['verify_binding_identities'](lam)
 def test_denied_native_identity_does_not_receive_acceptance(self):
  m=self.load();lam=Mock();lam.get_function_configuration.side_effect=PermissionError('invented denial')
  with self.assertRaises(PermissionError):m['verify_binding_identities'](lam)

if __name__=='__main__':unittest.main()
