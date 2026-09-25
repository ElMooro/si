from pathlib import Path
from unittest.mock import Mock, MagicMock, patch
import copy, sys, unittest
ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT / p) for p in ('aws/ops/checks', 'aws/ops/staged', 'tests')]
import ops_6068_sec_retained_history_qualification as audit
from test_sec_ftd_complete_archives import fixture, STAMP, TEXT, zipped


def complete_fixture():
    client, baseline, urls = fixture()
    baseline_ref = audit.base.retain(client, audit.raw.encoded(baseline))
    client.data[audit.base.STATUS] = audit.raw.encoded({'status': 'complete', 'manifest': baseline_ref})
    captures = {url: copy.deepcopy(baseline['captures'][str(i+1)]) for i, url in enumerate(urls[:2])}
    for url in urls[2:]:
        year, month, half = audit.sec.archive_period(url)
        first, second = (15, 16) if half == 'b' else (3, 4)
        body = TEXT.replace(b'20260817', f'{year}{month:02d}{first:02d}'.encode()).replace(b'20260818', f'{year}{month:02d}{second:02d}'.encode())
        body = body.replace(b'|ABC|', b'|U116SPINOFF|')
        member = f'cnsfails{year}{month:02d}{half}' + ('.txt' if month >= 5 else '')
        captures[url] = {'url': url, 'http_status': 200, 'status': 'response_retained',
            'requested_at': STAMP, 'received_at': STAMP, 'headers': {},
            'original': audit.base.retain(client, zipped(body, name=member)),
            'source_validation': {'status': 'failed', 'reason': 'Old parser assumption'}}
    failed = {'request_id': audit.previous.REQUEST, 'status': 'failed', 'baseline': baseline_ref, 'captures': captures}
    client.data[audit.previous.key('campaign')] = audit.raw.encoded(failed)
    return client, baseline, failed


class Tests(unittest.TestCase):
    def setUp(self):
        replacement = patch.object(audit.previous, 'BASELINE_PARSER_SHA', audit.raw.sha((ROOT / 'aws/ops/checks/sec_ftd_inventory.py').read_bytes()))
        replacement.start()
        self.addCleanup(replacement.stop)

    def test_complete_retained_population_qualifies_without_requests_or_native_writes(self):
        client, baseline, failed = complete_fixture()
        original_map = {u: v['original'] for u, v in failed['captures'].items()}
        lam = Mock()
        lam.get_function_configuration.return_value = {'FunctionArn': 'arn:fixture'}
        with patch.object(audit.diagnosis, 'ORIGINALS', original_map), \
             patch.object(audit.boto3, 'client', side_effect=lambda name, **kw: client if name == 's3' else lam), \
             patch.object(audit, 'runtime', return_value=baseline['runtime']), \
             patch.object(audit.base, 'bindings', return_value=baseline['bindings']), \
             patch.object(audit.previous, 'fetch', side_effect=AssertionError('No provider request')), \
             patch.object(audit, 'denied_with_retry', return_value=True), \
             patch.object(audit, 'report', return_value=MagicMock()):
            audit.main()
            audit.main()
        state = audit.raw.strict(client.data[audit.STATUS])
        manifest = audit.raw.strict(audit.base.checked(client, state['manifest']))
        self.assertEqual(len(manifest['captures']), 12)
        self.assertEqual(manifest['cross_archive_inventory']['unique_reported_date_cusips'], 36)
        self.assertEqual(manifest['cross_archive_inventory']['conflicting_cross_archive_records'], 0)
        self.assertTrue(all(v['inventory']['control_totals']['quantity_checksum_matches'] for v in manifest['captures'].values()))
        self.assertTrue(all(v['Key'].startswith(audit.PRIVATE) for v in client.writes))
        self.assertNotIn(audit.base.CURRENT, client.data)
        lam.invoke.assert_not_called()

    def test_changed_original_or_predecessor_inventory_cannot_be_requalified(self):
        for kind in ('original', 'inventory'):
            client, baseline, failed = complete_fixture()
            original_map = {u: v['original'] for u, v in failed['captures'].items()}
            first = next(iter(failed['captures'].values()))
            if kind == 'original':
                client.data[first['original']['key']] += b'changed'
            else:
                first['inventory']['rows'] = 999
            with patch.object(audit.diagnosis, 'ORIGINALS', original_map), \
                 self.subTest(kind=kind), self.assertRaises((ValueError, AssertionError)):
                audit.reconstruct(client, baseline, failed)


if __name__ == '__main__':
    unittest.main(verbosity=2)
