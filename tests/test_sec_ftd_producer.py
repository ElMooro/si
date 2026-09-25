from pathlib import Path
from io import BytesIO
from unittest.mock import patch
import sys, unittest, urllib.error
ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT / p) for p in ('aws/shared', 'tests')]
import sec_ftd_producer as producer
import sec_ftd_context as gate
from test_sec_ftd_research import fixture, STAMP, model
from test_option_flow_store import S3


class Transport:
    def __init__(self, fail=False, empty=False):
        blobs, inputs = fixture()
        captures = [inputs['index'], *inputs['captures'].values()]
        self.data = {cap['url']: (blobs[cap['original']['key']], cap['headers']) for cap in captures}
        self.calls, self.fail, self.empty = [], fail, empty
    def __call__(self, request, timeout):
        self.calls.append(request)
        if self.fail and request.full_url.endswith('.zip'):
            raise urllib.error.HTTPError(request.full_url, 503, 'Unavailable', {}, BytesIO(b'' if self.empty else b'unavailable'))
        data, headers = self.data[request.full_url]
        response = BytesIO(data)
        response.status, response.headers = 200, headers
        return response


class RaceS3(S3):
    def put_object(self, **request):
        if request['Key'] == model.CURRENT and request.get('IfMatch'):
            self.data[model.CURRENT] = b'{"other_writer":true}'
        return super().put_object(**request)


class Tests(unittest.TestCase):
    def previous(self):
        return model.encoded({'version': 'legacy', 'generated_at': '2026-09-24T00:00:00Z',
            'board': [{'ticker': 'ABC', 'state': 'LOADED', 'score': 99}]})

    def test_complete_collection_original_replay_and_one_conditional_head(self):
        client, transport = S3({model.CURRENT: self.previous()}), Transport()
        with patch.object(producer, 'now', return_value=STAMP), patch.object(producer.time, 'sleep'):
            result = producer.run(client, 'bucket', 'accepted', 'actual-execution', transport=transport)
            again = producer.run(client, 'bucket', 'accepted', 'different-execution', transport=transport)
        self.assertTrue(result['published'])
        self.assertEqual(result, again)
        self.assertEqual(len(transport.calls), 13)
        self.assertTrue(all(r.data is None and 'Authorization' not in r.headers for r in transport.calls))
        self.assertTrue(any('/data/other/' in r.full_url for r in transport.calls))
        writes = [v for v in client.writes if v['Key'] == model.CURRENT]
        self.assertEqual(len(writes), 1)
        self.assertEqual(writes[0]['CacheControl'], 'no-store')
        self.assertIn('IfMatch', writes[0])
        packet = model.strict(client.data[model.CURRENT])
        self.assertEqual(packet['counts']['original_rows'], 36)
        self.assertTrue(gate.context(packet)['native_reference_available'])
        self.assertIn(self.previous(), client.data.values())
        self.assertTrue(all(v['Key'] == model.CURRENT or v['Key'].startswith((model.PRIVATE, model.PREFIX)) for v in client.writes))

    def test_provider_failure_retains_exact_nonempty_and_empty_error_and_never_recollects(self):
        for empty in (False, True):
            old = self.previous()
            client, transport = S3({model.CURRENT: old}), Transport(fail=True, empty=empty)
            with self.subTest(empty=empty), patch.object(producer, 'now', return_value=STAMP), patch.object(producer.time, 'sleep'):
                with self.assertRaisesRegex(ValueError, 'SEC source unavailable'):
                    producer.run(client, 'bucket', 'failed', 'exec', transport=transport)
                calls = len(transport.calls)
                with self.assertRaisesRegex(ValueError, 'already attempted'):
                    producer.run(client, 'bucket', 'failed', 'exec', transport=transport)
            self.assertEqual(client.data[model.CURRENT], old)
            self.assertEqual(calls, 2)
            self.assertEqual(len(transport.calls), calls)
            self.assertIn(b'' if empty else b'unavailable', client.data.values())
            self.assertEqual(model.strict(client.data[producer.request_key('failed')])['status'], 'failed')

    def test_unqualified_source_and_damaged_replay_never_replace_predecessor(self):
        for phase in ('compile', 'replay'):
            old = self.previous()
            client, transport = S3({model.CURRENT: old}), Transport()
            module, name = (producer.model, 'compile_output') if phase == 'compile' else (producer.store, 'replay')
            with patch.object(producer, 'now', return_value=STAMP), patch.object(producer.time, 'sleep'), \
                 patch.object(module, name, side_effect=ValueError('Qualification failed')):
                with self.assertRaisesRegex(ValueError, 'Qualification failed'):
                    producer.run(client, 'bucket', phase, 'exec', transport=transport)
            self.assertEqual(client.data[model.CURRENT], old)
            self.assertFalse(any(v['Key'] == model.CURRENT for v in client.writes))

    def test_concurrent_writer_and_newer_observations_are_preserved(self):
        client = RaceS3({model.CURRENT: self.previous()})
        with patch.object(producer, 'now', return_value=STAMP), patch.object(producer.time, 'sleep'):
            result = producer.run(client, 'bucket', 'race', 'exec', transport=Transport())
        self.assertFalse(result['published'])
        self.assertEqual(result['reason'], 'concurrent_publication')
        self.assertEqual(client.data[model.CURRENT], b'{"other_writer":true}')
        self.assertFalse(producer.not_older({'generated_at': STAMP, 'settlement_date': '2026-08-31'},
            {'generated_at': '2026-09-24T00:00:00Z', 'settlement_date': '2026-09-15'}))

    def test_unreviewed_url_and_insufficient_budget_cannot_write_or_request(self):
        client, transport = S3({}), Transport()
        for url in ('https://example.com/', producer.source.INDEX + '?credential=secret'):
            with self.assertRaises(ValueError):
                producer.fetch(client, 'bucket', 'x', 'invalid', url, 1e20, transport)
        with self.assertRaises(ValueError):
            producer.run(client, 'bucket', 'x', 'exec', remaining_seconds=100, transport=transport)
        self.assertEqual(client.writes, [])
        self.assertEqual(transport.calls, [])


if __name__ == '__main__':
    unittest.main(verbosity=2)
