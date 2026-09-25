from pathlib import Path
from io import BytesIO
from unittest.mock import Mock
import sys, unittest
ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT / 'aws/ops/staged'), str(ROOT / 'tests')]
import ops_6053_short_interest_source_baseline as audit
from test_option_flow_store import S3


class Tests(unittest.TestCase):
    def test_inventory_does_not_promote_undated_short_float_or_legacy_scores(self):
        value = {'by_ticker': {'A': {'short_interest': 0, 'days_to_cover': 1, 'settlement_date': '2026-09-15',
                                     'short_float_pct': 80, 'signal': 'SQUEEZE_RISK', 'price_window_days': 40},
                               'B': {'short_interest': None, 'short_float_pct': 20, 'short_float_as_of': None}}}
        actual = audit.inventory(value)
        self.assertEqual(actual['non_null_counts']['short_interest'], 1)
        self.assertEqual(actual['observation_dates']['short_float_as_of'], {'None': 2})
        self.assertEqual(actual['legacy_signal_counts']['SQUEEZE_RISK'], 1)
        self.assertFalse(actual['provider_originals_verified'])
        self.assertFalse(actual['legacy_signal_qualification'])

    def test_evidence_rejects_private_accounts_and_preserves_whole_originals(self):
        client = S3({})
        for key in ('portfolio/account.json', 'data/other.json', audit.PRIVATE + '../other'):
            with self.assertRaises(ValueError):
                audit.read(client, key)
        ref = audit.retain(client, b'{"complete":true}')
        self.assertEqual(audit.checked(client, ref), b'{"complete":true}')
        client.data[ref['key']] = b'changed'
        with self.assertRaises(AssertionError):
            audit.checked(client, ref)

    def test_discovery_makes_exactly_one_keyless_request_and_does_not_claim_coverage(self):
        client, calls = S3({}), []
        def transport(request, timeout):
            calls.append(request)
            response = BytesIO(b'{"availablePartitions":[]}')
            response.status, response.headers = 200, {'Content-Type': 'application/json', 'Set-Cookie': 'not retained'}
            return response
        result = audit.capture(client, 'partitions', transport)
        self.assertEqual(len(calls), 1)
        self.assertEqual(calls[0].full_url, audit.URLS['partitions'])
        self.assertIsNone(calls[0].data)
        self.assertNotIn('Authorization', calls[0].headers)
        self.assertNotIn('set-cookie', result['headers'])
        self.assertFalse(result['population_qualified'])
        with self.assertRaises(KeyError):
            audit.capture(client, 'private', transport)
        self.assertEqual(len(calls), 1)

    def test_non_function_named_scheduler_is_discovered_without_mutation(self):
        scheduler, events = Mock(), Mock()
        def paginator(name):
            value = Mock()
            value.paginate.return_value = [{'ScheduleGroups': [{'Name': 'default'}]}] if name == 'list_schedule_groups' else [{'Schedules': [{'Name': 'short-interest-sched', 'Target': {'Arn': 'arn:native'}}]}]
            return value
        scheduler.get_paginator.side_effect = paginator
        scheduler.get_schedule.return_value = {'Name': 'short-interest-sched', 'GroupName': 'default',
            'ScheduleExpression': 'cron(15 21 ? * MON,WED *)', 'ScheduleExpressionTimezone': 'UTC', 'State': 'ENABLED',
            'FlexibleTimeWindow': {'Mode': 'OFF'}, 'Target': {'Arn': 'arn:native', 'RoleArn': 'arn:role', 'Input': ''}}
        events.get_paginator.return_value.paginate.return_value = [{'RuleNames': []}]
        result = audit.bindings(scheduler, events, 'arn:native')
        self.assertEqual(result['schedules'][0]['name'], 'short-interest-sched')
        scheduler.update_schedule.assert_not_called()
        events.put_rule.assert_not_called()


if __name__ == '__main__':
    unittest.main(verbosity=2)
