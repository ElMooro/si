from pathlib import Path
from copy import deepcopy
from unittest.mock import Mock
import json,sys,unittest
ROOT=Path(__file__).resolve().parents[2]
sys.path[:0]=[str(ROOT/p) for p in ('aws/ops/staged','aws/ops','aws/ops/checks','scripts')]
import ops_6266_restore_opportunity_original_schedule as op
from normalize_lambda_config import normalize_config


class Tests(unittest.TestCase):
    def fixture(self):
        baseline={'schedules':[{'kind':'EventBridge Scheduler','name':'opportunity-screener-sched','state':'ENABLED','expression':'cron(0 14 * * ? *)','timezone':'UTC','native_targets':1,'group':'default'}]}
        actual={'schedules':[{'kind':'EventBridge rule','name':op.RULE,'state':'ENABLED','expression':'cron(0 14 * * ? *)','native_targets':1}]+deepcopy(baseline['schedules'])}
        rule={'Name':op.RULE,'Arn':op.RULE_ARN,'State':'ENABLED','ScheduleExpression':'cron(0 14 * * ? *)','EventBusName':'default'}
        return actual,baseline,rule,[{'Id':op.ID,'Arn':op.ARN}],None
    def test_old_config_would_create_rule_and_new_reference_preserves_original_scheduler(self):
        old=json.loads((ROOT/'tests/fixtures/pre-opportunity-scheduler-reference-config.json.txt').read_bytes());new=json.loads((ROOT/'aws/lambdas'/op.FN/'config.json').read_bytes())
        self.assertEqual(normalize_config(old)['schedule']['rule_name'],op.RULE)
        fixed=normalize_config(new);self.assertNotIn('schedule',fixed);self.assertEqual(fixed['release_schedule_note']['schedule_name'],'opportunity-screener-sched');self.assertEqual(fixed['release_schedule_note']['binding_action'],'PRESERVE_EXISTING')
    def test_only_exact_new_target_and_unchanged_original_schedule_may_be_restored(self):
        args=self.fixture();self.assertEqual(op.plan(*args)['targets'],args[3])
        for index,mutation in [(0,lambda x:x['schedules'][-1].update(expression='rate(1 minute)')),(2,lambda x:x.update(EventPattern='{}')),(3,lambda x:x[0].update(Input='private payload')),(3,lambda x:x.append({'Id':'another','Arn':'other'}))]:
            changed=deepcopy(args);mutation(changed[index])
            with self.assertRaises(ValueError):op.plan(*changed)
        args=list(self.fixture());args[-1]={'Sid':op.SID,'Resource':'foreign'}
        with self.assertRaises(ValueError):op.plan(*args)
    def test_only_expected_rule_permission_is_removed_with_policy_revision_guard(self):
        args=list(self.fixture());args[-1]={'Sid':op.SID,'Effect':'Allow','Principal':{'Service':'events.amazonaws.com'},'Action':'lambda:InvokeFunction','Resource':op.ARN,'Condition':{'ArnLike':{'AWS:SourceArn':op.RULE_ARN}}}
        evidence=op.plan(*args);evidence['permission_revision']='fixture-revision'
        events=Mock();lam=Mock();events.describe_rule.side_effect=[evidence['rule'],dict(evidence['rule'],State='DISABLED')];events.list_targets_by_rule.side_effect=[{'Targets':evidence['targets']},{'Targets':[]}];events.remove_targets.return_value={'FailedEntryCount':0}
        op.restore(events,lam,evidence);lam.remove_permission.assert_called_once_with(FunctionName=op.FN,StatementId=op.SID,RevisionId='fixture-revision')
    def test_restore_disables_and_detaches_only_extra_rule_no_original_scheduler_or_invocation(self):
        evidence=op.plan(*self.fixture());events=Mock();lam=Mock();events.describe_rule.side_effect=[dict(evidence['rule'],ResponseMetadata={'RequestId':'new'}),dict(evidence['rule'],State='DISABLED')];events.list_targets_by_rule.side_effect=[{'Targets':evidence['targets']},{'Targets':[]}];events.remove_targets.return_value={'FailedEntryCount':0}
        op.restore(events,lam,evidence);events.disable_rule.assert_called_once_with(Name=op.RULE);events.remove_targets.assert_called_once_with(Rule=op.RULE,Ids=[op.ID]);self.assertFalse(lam.mock_calls)
        events=Mock();events.describe_rule.return_value=dict(evidence['rule'],State='DISABLED');events.list_targets_by_rule.return_value={'Targets':evidence['targets']}
        with self.assertRaises(ValueError):op.restore(events,lam,evidence)
        events.disable_rule.assert_not_called()


if __name__=='__main__':unittest.main(verbosity=2)
