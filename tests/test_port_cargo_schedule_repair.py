from pathlib import Path
from copy import deepcopy
from unittest.mock import Mock, patch
import json, sys, unittest
ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT/p) for p in ('aws/ops/staged', 'aws/ops', 'aws/ops/checks', 'scripts')]
with patch('boto3.client'):
    import ops_6211_port_cargo_schedule_repair as op


class Tests(unittest.TestCase):
    def fixture(self):
        rule = {'Name': op.NAME, 'Arn': op.RULE_ARN, 'ScheduleExpression': op.EXPRESSION,
                'State': 'ENABLED', 'Description': op.DESCRIPTION, 'EventBusName': 'default'}
        targets = {'Targets': [{'Id': 'audit-'+op.FN, 'Arn': op.ARN}]}
        original = {'Name': op.NAME, 'GroupName': 'default', 'State': 'ENABLED',
                    'ScheduleExpression': op.EXPRESSION, 'ScheduleExpressionTimezone': 'UTC',
                    'Target': {'Arn': op.ARN, 'Input': '{"preserve":"original"}', 'RetryPolicy': {'MaximumRetryAttempts': 2}},
                    'FlexibleTimeWindow': {'Mode': 'OFF'}}
        events, scheduler, retain = Mock(), Mock(), Mock(return_value={'retained': True})
        events.describe_rule.side_effect = lambda **kw: deepcopy(rule)
        events.list_targets_by_rule.side_effect = lambda **kw: deepcopy(targets)
        events.disable_rule.side_effect = lambda **kw: rule.update(State='DISABLED')
        scheduler.get_schedule.side_effect = lambda **kw: deepcopy(original)
        return events, scheduler, retain, rule, targets, original

    def test_config_only_corrects_service_identity_and_preserves_all_original_fields(self):
        old = json.loads((ROOT/'tests/fixtures/pre-scheduler-port-cargo-config.json.txt').read_bytes())
        current = json.loads((ROOT/'aws/lambdas/justhodl-port-cargo/config.json').read_bytes())
        restored = deepcopy(current); restored['schedule']['name'] = restored['schedule'].pop('scheduler_name')
        self.assertEqual(restored, old)
        self.assertEqual(op.normalize_config(old)['release_schedule_note']['status'], 'MANAGED_CLASSIC_RULE')
        normalized = op.normalize_config(current)
        self.assertNotIn('schedule', normalized)
        self.assertEqual(normalized['release_schedule_note'], {'status': 'EXISTING_SCHEDULER_REFERENCE',
            'schedule_name': op.NAME, 'configured_expression': op.EXPRESSION, 'binding_action': 'PRESERVE_EXISTING'})

    def test_only_exact_extra_rule_is_disabled_original_and_targets_are_unchanged(self):
        events, scheduler, retain, rule, targets, original = self.fixture()
        prior = deepcopy(original); target_prior = deepcopy(targets)
        result = op.quarantine(events, scheduler, retain)
        self.assertTrue(result['duplicate_rule_disabled']); self.assertTrue(result['changed_this_run'])
        events.disable_rule.assert_called_once_with(Name=op.NAME)
        events.delete_rule.assert_not_called(); events.remove_targets.assert_not_called()
        scheduler.update_schedule.assert_not_called(); scheduler.delete_schedule.assert_not_called()
        self.assertEqual(original, prior); self.assertEqual(targets, target_prior)
        self.assertEqual(retain.call_args.args[0]['rule']['State'], 'ENABLED')
        self.assertEqual(retain.call_args.args[0]['original_scheduler'], prior)
        self.assertFalse(op.quarantine(events, scheduler, retain)['changed_this_run'])
        events.disable_rule.assert_called_once()

    def test_unknown_targets_rule_fields_and_incomplete_inventory_refuse_mutation(self):
        for case in ('other_arn', 'extra_target', 'input', 'pagination', 'pattern', 'role', 'cadence', 'description', 'state'):
            events, scheduler, retain, rule, targets, _ = self.fixture()
            if case == 'other_arn': targets['Targets'][0]['Arn'] += ':live'
            elif case == 'extra_target': targets['Targets'].append({'Id': 'another', 'Arn': op.ARN})
            elif case == 'input': targets['Targets'][0]['Input'] = '{"other":true}'
            elif case == 'pagination': targets['NextToken'] = 'more'
            elif case == 'pattern': rule['EventPattern'] = '{}'
            elif case == 'role': rule['RoleArn'] = 'another'
            elif case == 'cadence': rule['ScheduleExpression'] = 'rate(5 minutes)'
            elif case == 'description': rule['Description'] = 'another owner'
            elif case == 'state': rule['State'] = 'UNKNOWN'
            with self.assertRaises(ValueError): op.quarantine(events, scheduler, retain)
            events.disable_rule.assert_not_called(); retain.assert_not_called()

    def test_original_schedule_identity_or_payload_drift_refuses_disable(self):
        for case in ('identity', 'state', 'drift', 'retention'):
            events, scheduler, retain, _, _, original = self.fixture()
            if case == 'identity': original['Target']['Arn'] += ':live'
            elif case == 'state': original['State'] = 'DISABLED'
            elif case == 'drift': retain.side_effect = lambda value: original['Target'].update(Input='changed') or {}
            else: retain.side_effect = RuntimeError('retention failed')
            with self.assertRaises((ValueError, RuntimeError)): op.quarantine(events, scheduler, retain)
            events.disable_rule.assert_not_called()

    def test_extra_rule_change_before_disable_and_failed_readback_are_not_success(self):
        events, scheduler, retain, rule, _, _ = self.fixture()
        retain.side_effect = lambda value: rule.update(Description='new owner') or {}
        with self.assertRaises(ValueError): op.quarantine(events, scheduler, retain)
        events.disable_rule.assert_not_called()
        events, scheduler, retain, _, _, _ = self.fixture()
        events.disable_rule.side_effect = None
        with self.assertRaises(ValueError): op.quarantine(events, scheduler, retain)

    def test_baseline_requires_all_original_runtime_and_exact_schedule_set(self):
        baseline = json.loads((ROOT/'docs/audit/2026-09-27/shipping-consumer-baseline.json').read_bytes())['consumer_runtimes'][op.FN]
        actual = {'runtime': 'python3.12', 'handler': 'lambda_function.lambda_handler', 'memory_mb': 1536,
                  'timeout': 780, 'architectures': ['x86_64'], 'role': baseline['runtime']['Role'], 'ephemeral_storage_mb': 512,
                  'schedules': [{'kind': 'EventBridge rule', 'name': op.NAME, 'state': 'DISABLED', 'expression': op.EXPRESSION, 'native_targets': 1}] + deepcopy(baseline['schedules'])}
        op.verify_baseline(actual, baseline, 'DISABLED')
        for key, value in [('timeout', 900), ('ephemeral_storage_mb', 1024), ('schedules', []), ('memory_mb', 2048)]:
            with self.assertRaises(ValueError): op.verify_baseline({**actual, key: value}, baseline, 'DISABLED')


if __name__ == '__main__': unittest.main(verbosity=2)
