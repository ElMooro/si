from pathlib import Path
import copy,runpy,sys,types,unittest
from unittest.mock import Mock,patch
R=Path(__file__).resolve().parents[3];FN='justhodl-contract-gate'
CFG={'FunctionName':FN,'FunctionArn':'invented-arn','CodeSha256':'invented-code-hash','State':'Active','LastUpdateStatus':'Successful','Runtime':'python3.12','Handler':'lambda_function.lambda_handler','Timeout':180,'MemorySize':512,'Architectures':['x86_64'],'Role':'invented-role','EphemeralStorage':{'Size':512},'Environment':{'Variables':{'PRIVATE_CANARY':'DO_NOT_RETURN'}}}
class Baseline(unittest.TestCase):
 def load(self):
  helper=types.SimpleNamespace(schedule_evidence=Mock(return_value=[{'kind':'EventBridge rule','name':'invented-hourly','state':'ENABLED','expression':'cron(25 * * * ? *)','native_targets':1}]),verified_alias=Mock(return_value=None))
  with patch.dict(sys.modules,{'market_runtime_evidence':helper}):m=runpy.run_path(str(R/'aws/ops/staged/ops_6446_heartbeat_controls.py'))
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
 def test_complete_empty_schedule_census_does_not_claim_other_route_verification(self):
  m,h=self.load();h.schedule_evidence.return_value=[];lam=Mock();lam.get_function_configuration.return_value=copy.deepcopy(CFG)
  out=m['capture'](lam,Mock(),Mock(),FN);self.assertEqual(out['schedule_observation'],'no_binding_observed_in_complete_schedule_inventory');self.assertFalse(out['other_invocation_routes_verified'])
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
  m['main'].__globals__['ScheduleInventory']=lambda client:client
  external={'boto3':types.SimpleNamespace(client=Mock(side_effect=lambda *a,**kw:object())), 'ops_report':types.SimpleNamespace(report=Mock(return_value=manager))}
  return m,out,calls,external
 def test_main_captures_every_named_function_twice_without_invoking(self):
  m,out,calls,external=self.main_fixture()
  with patch.dict(sys.modules,external):m['main']()
  self.assertEqual(calls,list(m['FUNCTIONS'])*2);self.assertEqual(len(m['FUNCTIONS']),7);self.assertEqual(len(set(m['FUNCTIONS'])),7)
  evidence=out.kv.call_args.kwargs['evidence'];self.assertFalse(evidence['normal_publication_verified']);self.assertFalse(evidence['investment_authority'])
  for key in ['native_invocations','provider_requests','application_packet_reads','private_reads','account_reads','native_writes','schedule_changes']:self.assertEqual(evidence[key],0)
  self.assertEqual([c.args[0] for c in external['boto3'].client.call_args_list],['lambda','events','scheduler'])
 def test_changed_controls_prevent_success_evidence(self):
  m,out,calls,external=self.main_fixture(changed=True)
  with patch.dict(sys.modules,external),self.assertRaises(ValueError):m['main']()
  out.kv.assert_not_called()

class ScheduleMetadata(unittest.TestCase):
 load=Baseline.load
 def inventory(self,pages):
  m,_=self.load();client=Mock();client.get_paginator.return_value.paginate.return_value=iter(pages)
  return m,client
 def test_census_retains_all_pages_and_drops_unneeded_fields(self):
  pages=[{'Schedules':[{'Name':'first','GroupName':'default','Target':{'Arn':'invented-arn','Input':'PRIVATE_CANARY'},'Private':'PRIVATE_CANARY'}]}, {'Schedules':[{'Name':'second','GroupName':'group','Target':{'Arn':'other-arn'}}]}]
  m,c=self.inventory(pages);view=m['ScheduleInventory'](c);rows=list(view.get_paginator('list_schedules').paginate())
  self.assertEqual(len(rows),2);self.assertNotIn('PRIVATE_CANARY',repr(rows));c.get_schedule.assert_not_called()
  self.assertEqual([r['Schedules'][0]['Name'] for r in rows],['first','second'])
 def test_complete_empty_response_supported_but_no_response_is_not_absence(self):
  m,c=self.inventory([{'Schedules':[]}]);self.assertEqual(list(m['ScheduleInventory'](c).paginate()),[{'Schedules':[]}])
  m,c=self.inventory([])
  with self.assertRaises(ValueError):m['ScheduleInventory'](c)
 def test_denied_later_page_does_not_produce_partial_inventory(self):
  m,c=self.inventory([])
  def pages():
   yield {'Schedules':[]}
   raise PermissionError('invented denial')
  c.get_paginator.return_value.paginate.return_value=pages()
  with self.assertRaises(PermissionError):m['ScheduleInventory'](c)
 def test_missing_malformed_and_duplicate_metadata_refused(self):
  row={'Name':'test','GroupName':'default','Target':{'Arn':'invented'}}
  for pages in [[{}],[{'Schedules':None}],[{'Schedules':[{'Name':'test'}]}],[{'Schedules':[row,row]}],[{'Schedules':[{**row,'Target':{}}]}]]:
   with self.subTest(pages=pages):
    m,c=self.inventory(pages)
    with self.assertRaises(ValueError):m['ScheduleInventory'](c)
 def test_census_cannot_be_silently_narrowed(self):
  m,c=self.inventory([{'Schedules':[]}]);view=m['ScheduleInventory'](c)
  with self.assertRaises(ValueError):view.get_paginator('unreviewed')
  with self.assertRaises(ValueError):view.paginate(NamePrefix='one-function')
 def test_selected_detail_reads_are_fresh_and_arguments_preserved(self):
  m,c=self.inventory([{'Schedules':[]}]);view=m['ScheduleInventory'](c);c.get_schedule.side_effect=[{'version':1},{'version':2}]
  self.assertNotEqual(view.get_schedule(Name='named',GroupName='default'),view.get_schedule(Name='named',GroupName='default'))
  self.assertEqual(c.get_schedule.call_count,2)
 def test_each_snapshot_reacquires_inventory_and_every_named_control(self):
  m,c=self.inventory([{'Schedules':[]}]);capture=Mock(side_effect=lambda *args:{'function':args[-1]})
  m['snapshot'].__globals__['capture']=capture
  c.get_paginator.return_value.paginate.side_effect=[iter([{'Schedules':[]}]),iter([{'Schedules':[]}])]
  self.assertEqual(m['snapshot'](Mock(),Mock(),c),m['snapshot'](Mock(),Mock(),c))
  self.assertEqual(c.get_paginator.return_value.paginate.call_count,2);self.assertEqual(capture.call_count,14)

if __name__=='__main__':unittest.main(verbosity=2)
