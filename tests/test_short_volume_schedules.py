from pathlib import Path
from unittest.mock import Mock
import sys, unittest
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'aws/ops/staged'))
import ops_6050_short_volume_schedule_baseline as op


class Tests(unittest.TestCase):
    def test_schedule_evidence_retains_cadence_and_hashes_input_without_disclosing_it(self):
        row = {'Name': 'finra-short-sched', 'GroupName': 'default', 'ScheduleExpression': 'cron(0 1 ? * TUE-SAT *)',
               'ScheduleExpressionTimezone': 'UTC', 'State': 'ENABLED', 'FlexibleTimeWindow': {'Mode': 'OFF'},
               'Target': {'Arn': 'native', 'RoleArn': 'role', 'Input': 'sensitive configuration'}}
        result = op.schedule_row(row, 'native')
        self.assertEqual(result['expression'], row['ScheduleExpression'])
        self.assertNotIn('sensitive configuration', str(result))
        self.assertEqual(len(result['input_sha256']), 64)
        with self.assertRaises(ValueError):
            op.schedule_row(row, 'different')

    def test_full_group_scan_finds_non_function_named_schedules(self):
        scheduler, events = Mock(), Mock()
        arns = {name: 'arn:' + name for name in op.NAMES}
        scheduler.get_paginator.return_value.paginate.side_effect = [
            [{'ScheduleGroups': [{'Name': 'default'}, {'Name': 'other'}]}],
            [{'Schedules': [{'Name': item[1], 'GroupName': item[0], 'Target': {'Arn': arns[name]}} for name, item in op.NAMES.items()]}],
            [{'Schedules': [{'Name': 'unrelated', 'Target': {'Arn': 'another'}}]}]]
        def actual(Name, GroupName):
            name = next(name for name, item in op.NAMES.items() if item[1] == Name)
            return {'Name': Name, 'GroupName': GroupName, 'ScheduleExpression': 'rate(1 day)', 'ScheduleExpressionTimezone': 'UTC',
                    'State': 'ENABLED', 'FlexibleTimeWindow': {'Mode': 'OFF'}, 'Target': {'Arn': arns[name], 'RoleArn': 'role'}}
        scheduler.get_schedule.side_effect = actual
        events.get_paginator.return_value.paginate.return_value = [{'RuleNames': []}]
        out = op.scan(scheduler, events, arns)
        self.assertEqual(out['schedulers_scanned'], 3)
        self.assertEqual(out['scheduler_groups_scanned'], ['default', 'other'])
        self.assertEqual(len(out['schedules']['justhodl-finra-short']), 1)
        scheduler.update_schedule.assert_not_called()
        scheduler.create_schedule.assert_not_called()


if __name__ == '__main__':
    unittest.main(verbosity=2)
