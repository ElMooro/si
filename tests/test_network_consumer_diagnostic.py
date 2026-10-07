"""Ensure the acceptance probe stays read-only and does not publish log data."""
from datetime import datetime, timezone
from pathlib import Path
import importlib.util
import json
import sys
import unittest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'aws/ops/staged'))
import ops_6492_network_consumer_diagnostic as probe
import ops_6490_network_schedule_diagnostic as schedules


class ReadOnly:
    def __init__(self): self.calls = []
    def list_rule_names_by_target(self, **kw):
        self.calls.append(('rules', kw)); return {'RuleNames': []}
    def list_schedules(self, **kw):
        self.calls.append(('schedules', kw)); return {'Schedules': []}
    def get_metric_statistics(self, **kw):
        self.calls.append(('metrics', kw)); return {'Datapoints': []}
    def filter_log_events(self, **kw):
        self.calls.append(('logs', kw))
        return {'events': [{'timestamp': 42, 'message': '[ERROR] TypeError: SECRET ACCOUNT DATA\n'
                 '  File "/var/task/lambda_function.py", line 143, in lambda_handler\n'
                 '    private_value(secret_token)'}], 'nextToken': 'secret-token'}


class DiagnosticTests(unittest.TestCase):
    def test_exact_targets_and_read_only_surface(self):
        client = ReadOnly()
        result = probe.collect(client, client, client, client, datetime.now(timezone.utc))
        self.assertEqual(set(result['functions']), set(probe.FUNCTIONS))
        self.assertEqual(result['native_invocations'], 0)
        self.assertEqual(result['cloud_writes'], 0)
        self.assertEqual(result['private_packet_reads'], 0)
        self.assertEqual(result['application_error_log_reads'], 2)
        self.assertEqual(len([x for x in client.calls if x[0] == 'metrics']), 21)
        self.assertEqual({x[1]['logGroupName'] for x in client.calls if x[0] == 'logs'},
                         {'/aws/lambda/' + name for name in probe.ERROR_FUNCTIONS})
        self.assertTrue(all(x[1]['limit'] == 50 for x in client.calls if x[0] == 'logs'))
        encoded = json.dumps(result)
        for secret in ('SECRET', 'ACCOUNT DATA', 'secret-token', 'private_value', 'secret_token'):
            self.assertNotIn(secret, encoded)
        self.assertTrue(result['error_observations'][probe.ERROR_FUNCTIONS[0]]['page_has_continuation'])

    def test_projection_does_not_copy_paths_code_or_exception_messages(self):
        event = {'timestamp': 1, 'message': 'TypeError: bearer SECRET\n'
                 'File "/var/task/lambda_function.py", line 12\n'
                 'File "/tmp/customer_private.py", line 2\n'
                 'File "/var/task/nested/private.py", line 4\n'
                 'File "/var/task/secret?token.py", line 8'}
        self.assertEqual(probe.error_projection(event), {'at_unix_ms': 1,
            'error_classes': ['TypeError'], 'frames': [{'file': 'lambda_function.py', 'line': 12}],
            'raw_message_retained': False})

    def test_empty_metrics_remain_unknown(self):
        client = ReadOnly()
        result = probe.collect(client, client, client, client, datetime.now(timezone.utc))
        self.assertEqual(result['functions'][probe.FUNCTIONS[0]]['metrics']['Errors'], [])
        self.assertIn('unknown, not zero', result['scope'])

    def test_existing_two_function_probe_default_unchanged(self):
        client = ReadOnly()
        result = schedules.collect(client, client, client, datetime.now(timezone.utc))
        self.assertEqual(set(result['functions']), set(schedules.FUNCTIONS))
        self.assertEqual(result['application_log_reads'], 0)

    def test_pagination_cycle_rejected(self):
        with self.assertRaisesRegex(ValueError, 'incomplete_inventory'):
            list(schedules.pages(lambda **kw: {'rows': [], 'NextToken': 'same'}, 'rows'))


if __name__ == '__main__': unittest.main()
