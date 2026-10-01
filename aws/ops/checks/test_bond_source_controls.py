from pathlib import Path
from unittest.mock import patch
import copy,json,runpy,sys,types,unittest
R=Path(__file__).resolve().parents[3]
class NativeControls(unittest.TestCase):
 def load(self,schedules=None):
  self.schedules=schedules if schedules is not None else [{'kind':'EventBridge rule','name':'bond-trace-daily','state':'ENABLED','expression':'cron(0 21 ? * MON-FRI *)','native_targets':1}]
  helper=types.ModuleType('market_runtime_evidence');helper.schedule_evidence=lambda *a:copy.deepcopy(self.schedules);helper.verified_alias=lambda *a:None
  with patch.dict(sys.modules,{'market_runtime_evidence':helper}):mod=runpy.run_path(str(R/'aws/ops/staged/ops_6411_bond_source_controls_baseline.py'))
  mod['capture'].__globals__['ROOT']=R;return mod
 def cfg(self):return {'FunctionName':'justhodl-bond-trace','FunctionArn':'arn:aws:lambda:us-east-1:000000000000:function:justhodl-bond-trace','State':'Active','LastUpdateStatus':'Successful','CodeSha256':'invented-package-digest','Runtime':'python3.12','Handler':'lambda_function.lambda_handler','Timeout':120,'MemorySize':256,'Architectures':['x86_64'],'Role':'arn:aws:iam::000000000000:role/invented','EphemeralStorage':{'Size':512},'Environment':{'Variables':{'NOT_FOR_OUTPUT':'invented-sensitive-canary'}}}
 def capture(self,mod,cfg):
  fake=types.SimpleNamespace(get_function_configuration=lambda **kw:copy.deepcopy(cfg));return mod['capture'](fake,object(),object(),'justhodl-bond-trace')
 def test_native_settings_preserved_without_environment_values(self):
  value=self.capture(self.load(),self.cfg());self.assertEqual(value['Timeout'],120);self.assertEqual(value['MemorySize'],256);self.assertNotIn('Environment',value);self.assertNotIn('invented-sensitive-canary',json.dumps(value))
 def test_unstable_missing_or_wrong_identity_fails(self):
  for key,value in [('State','Pending'),('LastUpdateStatus','InProgress'),('FunctionName','different')]:
   cfg=self.cfg();cfg[key]=value
   with self.subTest(key=key),self.assertRaises(ValueError):self.capture(self.load(),cfg)
 def test_missing_schedule_is_not_accepted(self):
  with self.assertRaises(ValueError):self.capture(self.load([]),self.cfg())
 def test_census_error_is_not_treated_as_empty(self):
  mod=self.load();mod['capture'].__globals__['schedule_evidence']=lambda *a:(_ for _ in ()).throw(PermissionError('invented denial'))
  with self.assertRaises(PermissionError):self.capture(mod,self.cfg())
 def test_all_schedule_entries_retained_and_only_order_normalized(self):
  rows=[{'kind':'EventBridge Scheduler','name':'second','state':'DISABLED','expression':'rate(1 hour)','native_targets':1,'group':'other'},{'kind':'EventBridge rule','name':'first','state':'ENABLED','expression':'cron(0 21 ? * MON-FRI *)','native_targets':1}]
  result=self.capture(self.load(rows),self.cfg());self.assertCountEqual(result['schedules'],rows)
 def test_operation_never_calls_application_or_mutating_services(self):
  text=(R/'aws/ops/staged/ops_6411_bond_source_controls_baseline.py').read_text(encoding='utf-8')
  for call in ['.invoke(','.put_rule(','.update_function_', '.get_object(','.filter_log_events(','.get_parameter(']:self.assertNotIn(call,text)
  self.assertIn('sys.exit(1)',text)
if __name__=='__main__':unittest.main()
