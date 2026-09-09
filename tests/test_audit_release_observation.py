import importlib.util
from pathlib import Path
import unittest
from types import SimpleNamespace
from unittest.mock import patch
ROOT=Path(__file__).resolve().parents[1]
spec=importlib.util.spec_from_file_location('observation',ROOT/'scripts/audit_release_observation.py');m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)
class BoundaryTests(unittest.TestCase):
 def test_sdk_mutations_are_rejected_before_resolving_underlying_client(self):
  class Spy:
   def __getattr__(self,name):raise AssertionError('underlying mutation resolved')
  for service in m.READ_METHODS:
   client=m.ReadOnlyClient(Spy(),service)
   for name in ('invoke','put_object','update_schedule','create_schedule','publish_version','delete_object','get_paginator'):
    with self.assertRaises(RuntimeError):getattr(client,name)
 def test_explicit_reads_remain_callable(self):
  for service,names in m.READ_METHODS.items():
   for name in names:
    client=m.ReadOnlyClient(SimpleNamespace(**{name:lambda **kw:kw}),service)
    self.assertEqual(getattr(client,name)(proof=True),{'proof':True})
 def test_source_failure_stops_before_output_or_schedule_observation(self):
  with patch.object(m.release,'changed_scope',return_value={'example':[]}),patch.object(m.release,'artifact_map',return_value={}),patch.object(m.release,'git',return_value='0'*40),patch.object(m.release,'check_packages',return_value=[{'function':'example','pass':False}]),patch.object(m.release,'privacy_receipt_summary',return_value={'verified':True}):
   result=m.observe(ROOT,{'lambda':None},None)
  self.assertEqual(result['status'],'SOURCE_PARITY_FAILED');self.assertNotIn('outputs',result)
 def test_schedule_discovery_retains_numeric_targets_and_withholds_input(self):
  events=SimpleNamespace(list_rules=lambda **kw:{'Rules':[{'Name':'alternate','State':'ENABLED','ScheduleExpression':'rate(1 hour)'}]},list_targets_by_rule=lambda **kw:{'Targets':[{'Arn':'arn:aws:lambda:us-east-1:857687956942:function:sample:23','Input':'PRIVATE_DATA'}]})
  scheduler=SimpleNamespace(list_schedules=lambda **kw:{'Schedules':[]})
  result=m.schedule_discovery({'events':events,'scheduler':scheduler},{'sample'})
  self.assertEqual(result['functions']['sample'][0]['target_arn'].split(':')[-1],'23')
  self.assertNotIn('PRIVATE_DATA',str(result))
 def test_source_evidence_is_checkpointed_before_later_metadata_failure(self):
  proof=[]
  with patch.object(m.release,'changed_scope',return_value={'example':[]}),patch.object(m.release,'artifact_map',return_value={'example':{'primary_keys':[]}}),patch.object(m.release,'git',return_value='0'*40),patch.object(m.release,'check_packages',return_value=[{'function':'example','pass':True}]),patch.object(m.release,'privacy_receipt_summary',return_value={'verified':True}),patch.object(m.release,'observe_schedules',side_effect=RuntimeError('metadata')):
   with self.assertRaises(RuntimeError):m.observe(ROOT,{'lambda':None},None,progress=lambda row:proof.append(row.copy()))
  self.assertTrue(proof[-1]['source_parity_verified']);self.assertEqual(proof[-1]['outputs'],{'example':[]})
 def test_unresolved_entries_are_rechecked_after_longer_metadata_scan(self):
  old={'function':'example','service':'scheduler','name':'hourly','status':'PENDING_CONFIGURATION'}
  new={**old,'status':'VERIFIED'}
  report={'schedules':[old],'outputs':{'example':[{'key':'data/example.json','status':'PENDING_OUTPUT'}]},'code':{'example':{'pass':True}}}
  with patch.object(m.release,'observe_schedules',return_value=[new]),patch.object(m.release,'inspect_output',return_value={'key':'data/example.json','status':'VERIFIED','observed_at':'2026-09-09T17:00:00Z'}) as inspect:
   m.reobserve_pending({'s3':None},ROOT,report)
  self.assertEqual(report['schedules'][0]['status'],'VERIFIED')
  self.assertEqual(report['outputs']['example'][0]['status'],'VERIFIED');self.assertEqual(inspect.call_count,1)
if __name__=='__main__':unittest.main()
