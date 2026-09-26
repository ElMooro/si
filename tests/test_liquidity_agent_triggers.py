from pathlib import Path
from datetime import datetime,timezone
import json,sys,unittest
from unittest.mock import Mock
ROOT=Path(__file__).resolve().parents[1]
sys.path[:0]=[str(ROOT/p) for p in ('aws/shared','aws/ops/checks','aws/ops/staged')]
import liquidity_agent_triggers as triggers
import risk_gate_runtime_diagnostic as diagnostic


class Tests(unittest.TestCase):
    def test_unexpected_name_group_and_old_version_still_identify_the_target(self):
        arn='arn:aws:lambda:us-east-1:123:function:justhodl-liquidity-agent'
        lam,scheduler,s3=(Mock() for _ in range(3))
        def pages(name):
            return Mock(paginate=Mock(return_value=[{'ScheduleGroups':[{'Name':'old-group'}]}] if name=='list_schedule_groups' else
                [{'Schedules':[{'Name':'unrelated-name','Target':{'Arn':arn+':7'}},
                    {'Name':'wrong-target','Target':{'Arn':arn+'-other'}}]}]))
        scheduler.get_paginator.side_effect=pages
        scheduler.get_schedule.return_value={'Name':'unrelated-name','GroupName':'old-group','State':'ENABLED',
            'ScheduleExpression':'rate(1 day)','Target':{'Arn':arn+':7','Input':'SECRET_PAYLOAD'}}
        lam.get_paginator.return_value.paginate.return_value=[{'EventSourceMappings':[]}]
        s3.get_bucket_notification_configuration.return_value={'LambdaFunctionConfigurations':[
            {'Id':'one','LambdaFunctionArn':arn,'Events':['s3:ObjectCreated:*'],'Filter':'SECRET_FILTER'}]}
        result=triggers.collect(lam,scheduler,s3,arn,'bucket')
        self.assertEqual(result['matching_schedules'][0]['target_arn'],arn+':7')
        self.assertEqual(result['schedules_scanned'],2)
        self.assertEqual(len(result['direct_bucket_notifications']),1)
        self.assertNotIn('SECRET',json.dumps(result))
        lam.invoke.assert_not_called();scheduler.update_schedule.assert_not_called();s3.put_object.assert_not_called()
        self.assertFalse(result['indirect_lambda_stepfunction_and_eventbus_callers_verified'])

    def test_diagnostic_function_boundary_precedes_any_aws_request(self):
        clients=[Mock() for _ in range(5)]
        with self.assertRaises(ValueError):diagnostic.collect(*clients,datetime.now(timezone.utc),'private-account-function')
        clients[0].get_function_configuration.assert_not_called()

    def test_new_operation_is_claimed_read_only_and_exits_failure(self):
        import ops_6126_liquidity_agent_trigger_diagnostic as operation
        self.assertEqual(operation.baseline.FUNCTION,'justhodl-liquidity-agent')
        text=(ROOT/'aws/ops/staged/ops_6126_liquidity_agent_trigger_diagnostic.py').read_text(encoding='utf-8')
        self.assertIn("IfNoneMatch='*'",text);self.assertIn('sys.exit(1)',text)
        for forbidden in ('.invoke(','.put_rule(','.update_schedule(','.update_function_configuration('):self.assertNotIn(forbidden,text)


if __name__=='__main__':unittest.main(verbosity=2)
