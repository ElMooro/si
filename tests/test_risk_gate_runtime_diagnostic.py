"""No raw log text, environment values, target payloads or signed URLs leak."""
from pathlib import Path
from datetime import datetime,timezone
import importlib.util,json,sys,unittest
from unittest.mock import Mock
ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'aws/ops/checks'))
import risk_gate_runtime_diagnostic as diagnostic

class Tests(unittest.TestCase):
 def test_error_summary_uses_only_known_categories_hashes_and_numeric_resource_reports(self):
  events=[{'timestamp':1,'message':'[ERROR] ValueError: source compiler differs from reviewed code api_key=SECRET'},
   {'timestamp':2,'message':'REPORT RequestId: private-id\tDuration: 123.40 ms\tBilled Duration: 124 ms\tMemory Size: 1024 MB\tMax Memory Used: 217 MB'},
   {'timestamp':3,'message':'[ERROR] unknown secret user details'}]
  out=diagnostic.summary(events);raw=json.dumps(out)
  for value in ('SECRET','api_key','private-id','user details'):self.assertNotIn(value,raw)
  self.assertEqual(out['known_failure_counts'],{'source_compiler_mismatch':1})
  self.assertEqual(len(out['error_message_hashes']),2)
  self.assertEqual(out['runtime_reports'][0]['duration_ms'],123.4)
  self.assertEqual(out['runtime_reports'][0]['max_memory_mb'],217)
 def test_inventory_includes_old_numbered_rule_targets_without_invoking_or_publishing(self):
  arn='arn:aws:lambda:us-east-1:123:function:justhodl-risk-gate'
  lam,events,scheduler,logs,metrics=[Mock() for _ in range(5)]
  lam.get_function_configuration.return_value={'FunctionName':diagnostic.FUNCTION,'FunctionArn':arn,'Environment':{'Variables':{'TOKEN':'secret-env'}}}
  def lp(name):
   return Mock(paginate=Mock(return_value=[{'Aliases':[{'Name':'live','FunctionVersion':'7'}]}] if name=='list_aliases' else [{'Versions':[{'Version':'$LATEST'},{'Version':'6'},{'Version':'7'}]}]))
  lam.get_paginator.side_effect=lp
  def ep(name):
   if name=='list_rule_names_by_target':return Mock(paginate=Mock(side_effect=lambda **kw:[{'RuleNames':['old-target'] if kw['TargetArn']==arn+':6' else []}]))
   return Mock(paginate=Mock(return_value=[{'Targets':[{'Id':'old','Arn':arn+':6','Input':'secret-payload'}]}]))
  events.get_paginator.side_effect=ep;events.describe_rule.return_value={'State':'ENABLED','ScheduleExpression':'rate(1 hour)'}
  scheduler.get_paginator.return_value.paginate.return_value=[]
  logs.describe_log_streams.return_value={'logStreams':[{'logStreamName':'private-stream','lastEventTimestamp':123}]}
  logs.get_log_events.return_value={'events':[{'timestamp':123,'message':'private-log'}]}
  metrics.get_metric_statistics.return_value={'Datapoints':[]}
  out,raw=diagnostic.collect(lam,events,scheduler,logs,metrics,datetime(2026,9,25,tzinfo=timezone.utc))
  self.assertEqual(out['eventbridge_rules'][0]['targets'][0]['Arn'],arn+':6')
  for value in ('secret-env','secret-payload','private-stream','private-log'):self.assertNotIn(value,json.dumps(out))
  self.assertEqual(raw[0]['message'],'private-log')
  lam.invoke.assert_not_called();scheduler.update_schedule.assert_not_called();events.put_targets.assert_not_called()
 def test_staged_operation_has_no_invoke_provider_or_current_publication_path(self):
  source=(ROOT/'aws/ops/staged/ops_6120_risk_gate_refresh_diagnostic.py').read_text(encoding='utf-8')
  self.assertNotIn('.invoke(',source);self.assertNotIn('put_rule(',source);self.assertNotIn('api.stlouisfed',source)
  self.assertIn('sys.exit(1)',source);self.assertIn("IfNoneMatch='*'",source)

if __name__=='__main__':unittest.main(verbosity=2)
