from pathlib import Path
from unittest.mock import Mock, patch
import sys, unittest
ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT / 'aws/ops/staged'), str(ROOT / 'aws/shared'), str(ROOT / 'tests')]
import ops_6052_short_volume_native_acceptance as acceptance
from test_option_flow_store import S3
from test_short_volume_context import packet


class Tests(unittest.TestCase):
    def test_failed_durable_dispatch_is_never_sent_again(self):
        p = acceptance.producer
        request = 'chatgpt-short-volume-native-' + 'a' * 12 + '-1'
        status, dispatch = p.request_key(request), p.request_key(request + '-dispatch')
        client = S3({acceptance.model.CURRENT: b'{}',
                     status: acceptance.model.encoded({'status': 'failed'}),
                     dispatch: acceptance.model.encoded({'contract': 'short-volume-native-dispatch.v1',
                                                        'request_id': request, 'function': acceptance.FUNCTIONS[0], 'status': 'accepted_async'})})
        with patch.object(acceptance, 'invoke_when_available') as invoke:
            with self.assertRaisesRegex(AssertionError, 'never blindly'):
                acceptance.invoke(Mock(), client, 'a' * 40, acceptance.FUNCTIONS[0], acceptance.model.CURRENT)
            invoke.assert_not_called()

    def test_qualified_current_adopts_matching_request_without_invocation(self):
        value = packet()
        client = S3({acceptance.model.CURRENT: acceptance.model.encoded(value)})
        with patch.object(acceptance, 'completed_request', return_value={'adopted': True}) as completed, patch.object(acceptance, 'invoke_when_available') as invoke:
            self.assertEqual(acceptance.invoke(Mock(), client, 'a' * 40, acceptance.FUNCTIONS[0], acceptance.model.CURRENT), {'adopted': True})
            completed.assert_called_once()
            invoke.assert_not_called()
        self.assertEqual(client.writes, [])

    def test_profiles_reject_native_allocation_overrun_and_timeout(self):
        status = {'execution_id': 'a' * 8 + '-aaaa-aaaa-aaaa-' + 'a' * 12,
                  'result': {'generated_at': '2026-09-25T05:00:00Z'}}
        logs = Mock()
        logs.filter_log_events.return_value = {'events': [{'message': 'REPORT'}]}
        good = {'memory_mb': 1024, 'max_memory_mb': 700, 'duration_ms': 120000}
        for result in (dict(good, max_memory_mb=1024), dict(good, duration_ms=300000), dict(good, status='timeout')):
            with patch.object(acceptance, 'parse_runtime', return_value=result), self.assertRaises(AssertionError):
                acceptance.profile(logs, acceptance.FUNCTIONS[0], status)
        with patch.object(acceptance, 'parse_runtime', return_value=good):
            self.assertEqual(acceptance.profile(logs, acceptance.FUNCTIONS[0], status)['managed_reports'], [good])


if __name__ == '__main__':
    unittest.main(verbosity=2)
