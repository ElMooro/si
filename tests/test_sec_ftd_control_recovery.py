from pathlib import Path
from unittest.mock import MagicMock, Mock, patch
import copy, sys, unittest
ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT / p) for p in ('aws/ops/checks', 'aws/ops/staged', 'tests')]
import ops_6060_sec_control_trailer_baseline as audit
from test_sec_ftd_complete_archives import fixture as archives


def fixture():
    client, baseline, urls = archives()
    previous = b'{"board":[],"top_picks":[]}'
    failed = {'request_id': audit.base.REQUEST, 'status': 'failed', 'error_type': 'ValueError',
              'commit': audit.SOURCE_COMMIT, 'captures': {k: copy.deepcopy(v) for k, v in baseline['captures'].items() if k != '2'},
              'runtime': baseline['runtime'], 'related_runtime': baseline['runtime'], 'bindings': baseline['bindings'],
              'predecessor': {'original': audit.retain(client, previous), 'inventory': audit.base.inventory(audit.raw.strict(previous))}}
    failed['captures']['1'].pop('inventory')
    originals = {k: (v['original']['sha256'], v['original']['bytes']) for k, v in failed['captures'].items()}
    client.data[audit.base.STATUS] = audit.raw.encoded(failed)
    return client, failed, originals, baseline['captures']['2'], urls[:2]


class Tests(unittest.TestCase):
    def test_adoption_preserves_complete_originals_and_adds_reconciled_controls(self):
        client, failed, originals, _, urls = fixture()
        before = copy.deepcopy(failed)
        with patch.object(audit, 'ORIGINALS', originals):
            captures, cutoff, selected = audit.adopt(client, failed)
        self.assertEqual(failed, before)
        self.assertEqual(selected, urls)
        self.assertEqual(cutoff, '2026-09-25')
        self.assertEqual(captures['1']['original'], failed['captures']['1']['original'])
        self.assertTrue(captures['1']['inventory']['control_totals']['quantity_checksum_matches'])

    def test_wrong_failure_identity_commit_or_original_cannot_be_adopted(self):
        for mutate in (lambda f: f.update(status='complete'), lambda f: f.update(commit='different'),
                       lambda f: f['captures']['1']['original'].update(bytes=1)):
            client, failed, originals, _, _ = fixture()
            mutate(failed)
            with patch.object(audit, 'ORIGINALS', originals), self.assertRaises(AssertionError):
                audit.adopt(client, failed)

    def run_main(self, failed_response=False):
        client, failed, originals, missing, urls = fixture()
        old_status = client.data[audit.base.STATUS]
        lam = Mock()
        lam.get_function_configuration.return_value = {'FunctionArn': 'arn:native'}
        capture = Mock(return_value=missing)
        if failed_response:
            capture.return_value = {'url': urls[1], 'http_status': 403, 'status': 'provider_error_retained',
                                    'original': audit.retain(client, b'SEC response unavailable')}
        with patch.object(audit, 'ORIGINALS', originals), \
             patch.object(audit.boto3, 'client', side_effect=lambda name, **kw: client if name == 's3' else lam), \
             patch.object(audit, 'runtime', return_value=failed['runtime']), \
             patch.object(audit, 'bindings', return_value=failed['bindings']), \
             patch.object(audit.base, 'capture', capture), \
             patch.object(audit, 'denied_with_retry', return_value=True), \
             patch.object(audit.subprocess, 'check_output', return_value=b'complete original parser'), \
             patch.object(audit, 'report', return_value=MagicMock()):
            if failed_response:
                with self.assertRaisesRegex(AssertionError, 'Provider error retained'):
                    audit.main()
                with self.assertRaisesRegex(AssertionError, 'Never repeat'):
                    audit.main()
            else:
                audit.main()
                audit.main()
        self.assertEqual(capture.call_count, 1)
        self.assertEqual(capture.call_args.args[1], urls[1])
        self.assertEqual(client.data[audit.base.STATUS], old_status)
        self.assertTrue(all(v['Key'].startswith(audit.PRIVATE) for v in client.writes))
        lam.invoke.assert_not_called()
        return client

    def test_corrected_campaign_requests_only_the_missing_archive_once(self):
        client = self.run_main()
        status = audit.raw.strict(client.data[audit.STATUS])
        self.assertEqual(status['status'], 'complete')
        manifest = audit.raw.strict(audit.checked(client, status['manifest']))
        self.assertEqual(manifest['reused_provider_responses'], 2)
        self.assertEqual(len(manifest['captures']), 3)
        self.assertTrue(manifest['source_originals_reused_without_recollection'])

    def test_new_provider_failure_preserves_both_journals_and_cannot_recollect(self):
        client = self.run_main(True)
        status = audit.raw.strict(client.data[audit.STATUS])
        self.assertEqual(status['status'], 'failed')
        self.assertEqual(status['captures']['2']['http_status'], 403)


if __name__ == '__main__':
    unittest.main(verbosity=2)
