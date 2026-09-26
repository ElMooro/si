from pathlib import Path
import copy,sys,unittest
ROOT=Path(__file__).resolve().parents[1]
sys.path[:0]=[str(ROOT/p) for p in ('aws/ops/staged','aws/ops','aws/ops/checks')]
import ops_6167_ici_native_acceptance as operation

class Events:
    def __init__(self):
        self.row={key:None for key in ('Name','Arn','State','ScheduleExpression','EventPattern','RoleArn','Description')}
        self.row.update(Name=operation.RULE,Arn='retained-arn',State='DISABLED',ScheduleExpression=operation.CRON,Description='existing')
        self.targets=[{'Id':'existing','Arn':'retained-target','Input':'{"scheduled":true}','RetryPolicy':{'MaximumRetryAttempts':1}}];self.enables=0
    def describe_rule(self,Name):return dict(self.row)
    def list_targets_by_rule(self,Rule):return {'Targets':copy.deepcopy(self.targets)}
    def enable_rule(self,Name):self.enables+=1;self.row['State']='ENABLED'

class Tests(unittest.TestCase):
    def test_restore_only_state_and_preserve_full_target_payload_and_weekly_cron(self):
        client=Events();before=copy.deepcopy(client.row);targets=copy.deepcopy(client.targets)
        after=operation.restore_weekly(client,before,targets,True)
        self.assertEqual(after,{**before,'State':'ENABLED'});self.assertEqual(client.targets,targets);self.assertEqual(client.enables,1)

    def test_failed_acceptance_enabled_rule_wrong_schedule_or_concurrent_edit_never_enables(self):
        for scenario in ('not-accepted','already-enabled','wrong-cron','concurrent-rule','concurrent-target'):
            client=Events();before=copy.deepcopy(client.row);targets=copy.deepcopy(client.targets)
            accepted=scenario!='not-accepted'
            if scenario=='already-enabled':before['State']='ENABLED'
            if scenario=='wrong-cron':before['ScheduleExpression']='rate(1 minute)'
            if scenario=='concurrent-rule':client.row['Description']='changed'
            if scenario=='concurrent-target':client.targets[0]['Input']='changed'
            with self.subTest(scenario=scenario),self.assertRaises(ValueError):operation.restore_weekly(client,before,targets,accepted)
            self.assertEqual(client.enables,0)

if __name__=='__main__':unittest.main()
