from pathlib import Path
from unittest.mock import Mock, MagicMock, patch
import copy, sys, time, unittest
ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT / p) for p in ('aws/ops/checks', 'aws/ops/staged', 'tests')]
import ops_6062_sec_advertised_archive_history as audit
from test_sec_ftd_source_baseline import zipped, TEXT
from test_option_flow_store import S3
STAMP = '2026-09-25T07:00:00+00:00'


def fixture():
    s3 = S3({})
    urls = ['https://www.sec.gov/files/data/fails-deliver-data/cnsfails2026' + f'{month:02d}' + half + '.zip'
            for month in range(8, 2, -1) for half in ('b', 'a')]
    urls = [url.replace('/data/', '/data/other/') if '202605' in url else url for url in urls]
    def captured(url, body):
        return {'url': url, 'requested_at': STAMP, 'received_at': STAMP,
                'http_status': 200, 'status': 'response_retained',
                'headers': {}, 'original': audit.base.retain(s3, body)}
    captures = {'0': captured(audit.sec.INDEX, ''.join('<a href="' + url + '">archive</a>' for url in reversed(urls)).encode())}
    for i, url in enumerate(urls[:2], 1):
        body = TEXT if i == 1 else TEXT.replace(b'20260817', b'20260803').replace(b'20260818', b'20260804')
        packed = zipped(body)
        captures[str(i)] = captured(url, packed)
        captures[str(i)]['inventory'] = audit.sec.inventory(packed, url, STAMP[:10])
    baseline = {'contract': 'squeeze-settlement-source-baseline.v1', 'request_id': audit.base.REQUEST,
                'captures': captures, 'selection_cutoff': STAMP[:10], 'advertised_selected_archives': urls[:2],
                'inventory_parser': audit.base.retain(s3, (ROOT / 'aws/ops/checks/sec_ftd_inventory.py').read_bytes()),
                'runtime': {'receipt': {'status': 'matched', 'commit': 'fixture'}},
                'bindings': {'schedules': [], 'classic_default_bus_rules': []}}
    return s3, baseline, urls


class Tests(unittest.TestCase):
    def setUp(self):
        source = (ROOT / 'aws/ops/checks/sec_ftd_inventory.py').read_bytes()
        substitute = patch.object(audit, 'BASELINE_PARSER_SHA', audit.raw.sha(source))
        substitute.start()
        self.addCleanup(substitute.stop)

    def test_plan_reuses_two_exact_archives_and_selects_ten_more_without_requests(self):
        client, baseline, urls = fixture()
        chosen, retained, index, cutoff = audit.plan(client, baseline)
        self.assertEqual(chosen, urls)
        self.assertEqual(set(retained), set(urls[:2]))
        self.assertEqual(len(set(chosen) - set(retained)), 10)
        self.assertEqual(index['url'], audit.sec.INDEX)
        self.assertEqual(cutoff, STAMP[:10])

    def test_plan_rejects_changed_source_inventory_selection_or_parser(self):
        for change in (lambda b: b['captures']['1']['inventory'].update(rows=1),
                       lambda b: b.update(advertised_selected_archives=[]),
                       lambda b: b.update(selection_cutoff='2026-09-24'),
                       lambda b: b['inventory_parser'].update(sha256='0'*64)):
            client, baseline, _ = fixture()
            change(baseline)
            with self.assertRaises((ValueError, AssertionError)):
                audit.plan(client, baseline)

    def test_each_archive_request_is_claimed_once_and_originals_are_not_truncated(self):
        client, baseline, urls = fixture()
        calls = []
        def capture(s3, url):
            calls.append(url)
            result = copy.deepcopy(baseline['captures']['1'])
            return result
        result = audit.fetch(client, urls[0], time.monotonic() + 20, capture)
        self.assertEqual(result['inventory']['rows'], 3)
        with self.assertRaises(Exception):
            audit.fetch(client, urls[0], time.monotonic() + 20, capture)
        self.assertEqual(calls, [urls[0]])
        status = audit.raw.strict(client.data[audit.key('source:' + urls[0])])
        self.assertEqual(status['status'], 'complete')
        self.assertEqual(audit.base.checked(client, status['capture']['original']), zipped())

    def test_provider_error_is_retained_and_claim_cannot_be_retried(self):
        client, _, urls = fixture()
        calls = []
        def capture(s3, url):
            calls.append(url)
            return {'url': url, 'http_status': 403, 'status': 'provider_error_retained',
                    'original': audit.base.retain(s3, b'Provider unavailable')}
        with self.assertRaises(AssertionError):
            audit.fetch(client, urls[2], time.monotonic() + 20, capture)
        status = audit.raw.strict(client.data[audit.key('source:' + urls[2])])
        self.assertEqual(status['status'], 'failed')
        self.assertEqual(audit.base.checked(client, status['capture']['original']), b'Provider unavailable')
        with self.assertRaises(Exception):
            audit.fetch(client, urls[2], time.monotonic() + 20, capture)
        self.assertEqual(len(calls), 1)

    def test_campaign_failure_never_invokes_a_producer_and_cannot_recollect(self):
        client, baseline, urls = fixture()
        baseline_ref = audit.base.retain(client, audit.raw.encoded(baseline))
        client.data[audit.base.STATUS] = audit.raw.encoded({'status': 'complete', 'manifest': baseline_ref})
        lam = Mock()
        lam.get_function_configuration.return_value = {'FunctionArn': 'arn:native'}
        with patch.object(audit.boto3, 'client', side_effect=lambda name, **kw: client if name == 's3' else lam), \
             patch.object(audit, 'runtime', return_value=baseline['runtime']), \
             patch.object(audit.base, 'bindings', return_value=baseline['bindings']), \
             patch.object(audit, 'fetch', side_effect=RuntimeError('transport unavailable')) as fetch, \
             patch.object(audit, 'report', return_value=MagicMock()):
            with self.assertRaisesRegex(RuntimeError, 'transport unavailable'):
                audit.main()
            with self.assertRaisesRegex(AssertionError, 'Never repeat'):
                audit.main()
        self.assertEqual(fetch.call_count, 1)
        self.assertEqual(audit.raw.strict(client.data[audit.key('campaign')])['status'], 'failed')
        self.assertTrue(all(value['Key'].startswith(audit.PRIVATE) for value in client.writes))
        self.assertNotIn(audit.base.CURRENT, client.data)
        lam.invoke.assert_not_called()

    def test_complete_history_reuses_two_and_fetches_only_ten_exact_advertised_urls(self):
        client, baseline, urls = fixture()
        ref = audit.base.retain(client, audit.raw.encoded(baseline))
        client.data[audit.base.STATUS] = audit.raw.encoded({'status': 'complete', 'manifest': ref})
        lam, requested = Mock(), []
        lam.get_function_configuration.return_value = {'FunctionArn': 'arn:native'}
        def capture(s3, url):
            requested.append(url)
            year, month, half = audit.sec.archive_period(url)
            a, b = (17, 18) if half == 'b' else (3, 4)
            body = TEXT.replace(b'20260817', f'{year}{month:02d}{a:02d}'.encode()).replace(b'20260818', f'{year}{month:02d}{b:02d}'.encode())
            return {'url': url, 'requested_at': STAMP, 'received_at': STAMP, 'http_status': 200,
                    'status': 'response_retained', 'headers': {}, 'original': audit.base.retain(s3, zipped(body))}
        fetch = audit.fetch
        with patch.object(audit.boto3, 'client', side_effect=lambda name, **kw: client if name == 's3' else lam), \
             patch.object(audit, 'runtime', return_value=baseline['runtime']), \
             patch.object(audit.base, 'bindings', return_value=baseline['bindings']), \
             patch.object(audit, 'fetch', side_effect=lambda s3, url, deadline: fetch(s3, url, deadline, capture)), \
             patch.object(audit.time, 'sleep'), patch.object(audit, 'denied_with_retry', return_value=True), \
             patch.object(audit, 'report', return_value=MagicMock()):
            audit.main()
            audit.main()
        self.assertEqual(requested, urls[2:])
        self.assertIn('/data/other/', requested[4])
        state = audit.raw.strict(client.data[audit.key('campaign')])
        manifest = audit.raw.strict(audit.base.checked(client, state['manifest']))
        self.assertEqual(manifest['selected_archives'], urls)
        self.assertEqual(len(manifest['captures']), 12)
        self.assertTrue(all(v['inventory']['control_totals']['quantity_checksum_matches'] for v in manifest['captures'].values()))
        lam.invoke.assert_not_called()


if __name__ == '__main__':
    unittest.main(verbosity=2)
