from pathlib import Path
from io import BytesIO
from unittest.mock import patch
import json, sys, unittest, urllib.error
ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT / p) for p in ('aws/shared', 'tests')]
import short_interest_producer as producer
import short_interest_context as gate
import short_interest_research_model as model
from test_short_interest_research import fixture, STAMP
from test_option_flow_store import S3


class Transport:
    def __init__(self, fail=False):
        blobs, inputs = fixture()
        self.data = {(cap['url'], model.encoded(cap['body'])): (blobs[cap['original']['key']], cap['headers']) for cap in inputs['captures'].values()}
        self.calls, self.fail = [], fail
    def __call__(self, request, timeout):
        self.calls.append(request)
        body = model.encoded(json.loads(request.data)) if request.data is not None else b'null'
        if self.fail and request.data:
            raise urllib.error.HTTPError(request.full_url, 503, 'Unavailable', {}, BytesIO(b'unavailable'))
        data, headers = self.data[(request.full_url, body)]
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
                              'settlement_date': '2026-08-31', 'by_ticker': {'ABC': {'signal': 'SQUEEZE_RISK', 'score': 99}}})

    def test_complete_collection_and_source_replay_precede_one_conditional_head(self):
        client, transport = S3({model.CURRENT: self.previous()}), Transport()
        with patch.object(producer, 'now', return_value=STAMP):
            result = producer.run(client, 'bucket', 'accepted', 'actual-aws-execution', transport=transport)
            again = producer.run(client, 'bucket', 'accepted', 'retry-execution', transport=transport)
        self.assertEqual(again, result)
        self.assertTrue(result['published'])
        self.assertEqual(len(transport.calls), 18)
        self.assertTrue(all('Authorization' not in r.headers for r in transport.calls))
        writes = [v for v in client.writes if v['Key'] == model.CURRENT]
        self.assertEqual(len(writes), 1)
        self.assertEqual(writes[0]['CacheControl'], 'no-store')
        self.assertIn('IfMatch', writes[0])
        packet = model.strict(client.data[model.CURRENT])
        self.assertEqual(packet['by_ticker'], {})
        self.assertEqual(packet['settlement_date'], '2026-09-15')
        self.assertTrue(any(value == self.previous() for key, value in client.data.items() if key.endswith('.bin')))

    def test_failed_source_does_not_overwrite_or_recollect_same_request(self):
        old = self.previous(); client, transport = S3({model.CURRENT: old}), Transport(fail=True)
        with patch.object(producer, 'now', return_value=STAMP):
            with self.assertRaises(ValueError):
                producer.run(client, 'bucket', 'failed', 'exec', transport=transport)
            count = len(transport.calls)
            with self.assertRaisesRegex(ValueError, 'already attempted'):
                producer.run(client, 'bucket', 'failed', 'exec', transport=transport)
        self.assertEqual(client.data[model.CURRENT], old)
        self.assertEqual(len(transport.calls), count)
        self.assertIn(b'unavailable', client.data.values())
        self.assertEqual(model.strict(client.data[producer.request_key('failed')])['status'], 'failed')

    def test_concurrent_head_and_newer_settlement_are_preserved(self):
        client = RaceS3({model.CURRENT: self.previous()})
        with patch.object(producer, 'now', return_value=STAMP):
            result = producer.run(client, 'bucket', 'race', 'exec', transport=Transport())
        self.assertFalse(result['published'])
        self.assertEqual(result['reason'], 'concurrent_publication')
        self.assertEqual(client.data[model.CURRENT], b'{"other_writer":true}')
        self.assertFalse(producer.not_older({'generated_at': STAMP, 'settlement_date': '2026-08-31'}, {'generated_at': '2026-09-24T00:00:00Z', 'settlement_date': '2026-09-15'}))

    def test_unreviewed_request_and_low_budget_have_no_transport_or_write(self):
        client, transport = S3({}), Transport()
        for url in ('https://example.com/', model.URLS['data'] + '?credential=private'):
            with self.assertRaises(ValueError):
                producer.fetch(client, 'bucket', 'x', 'bad', url, None, 1e20, transport)
        with self.assertRaises(ValueError):
            producer.run(client, 'bucket', 'x', 'exec', remaining_seconds=100, transport=transport)
        self.assertEqual(client.writes, [])
        self.assertEqual(transport.calls, [])

    def test_context_never_grants_descriptive_or_legacy_data_a_forecast_vote(self):
        legacy = {'by_ticker': {'ABC': {'score': 99, 'short_interest': 100}}, 'items': [{'utilization': 99}], 'call': 'BUY'}
        view = gate.decision_view(legacy)
        self.assertEqual(view['by_ticker'], {})
        self.assertEqual(view['items'], [])
        self.assertFalse(view['research_context']['native_reference_available'])
        self.assertEqual(view['independent_investment_votes'], 0)
        self.assertIs(gate.guard('data/unrelated.json', legacy), legacy)
        self.assertIsNone(gate.guard(model.CURRENT, legacy)['score'])


if __name__ == '__main__':
    unittest.main(verbosity=2)
