from pathlib import Path
from datetime import datetime, timezone
from io import BytesIO
from unittest.mock import patch
import sys, unittest, urllib.error
ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT / 'aws/shared'), str(ROOT / 'tests')]
import short_volume_producer as producer
import short_volume_research_model as model
from test_short_volume_research_model import fixture, DATES
from test_option_flow_store import S3


class FixedDateTime(datetime):
    @classmethod
    def now(cls, tz=None):
        return cls(2026, 9, 25, 4, 30, 0, tzinfo=timezone.utc)


class Transport:
    def __init__(self, fail=False, empty=False):
        inputs, objects = fixture()
        self.data = {v['url']: objects[v['original']['key']] for v in inputs['captures'].values()}
        self.calls = []
        self.fail, self.empty = fail, empty

    def __call__(self, request, timeout):
        url = request.full_url
        self.calls.append(url)
        if self.fail and '/daily/' in url:
            raise urllib.error.HTTPError(url, 503, 'Unavailable', {}, BytesIO(b'unavailable'))
        raw = b'' if self.empty and '/daily/' in url else self.data[url]
        response = BytesIO(raw)
        response.status = 200
        response.headers = {'Content-Length': str(len(raw))}
        return response


class RaceS3(S3):
    def put_object(self, **request):
        if request['Key'] == model.CURRENT and request.get('IfMatch'):
            self.data[model.CURRENT] = b'{"concurrent_writer":true}'
        return super().put_object(**request)


class Tests(unittest.TestCase):
    def previous(self):
        return model.encoded({'version': 'legacy', 'generated_at': '2026-09-25T01:00:00Z', 'data_date': '2026-09-24',
                              'squeeze_candidates': [{'ticker': 'AAPL', 'score': 99}]})

    def test_native_replays_every_original_before_one_conditional_publication(self):
        client = S3({model.CURRENT: self.previous()})
        transport = Transport()
        with patch.object(producer, 'datetime', FixedDateTime):
            result = producer.run(client, 'bucket', 'accepted', 'actual-execution', transport=transport)
            again = producer.run(client, 'bucket', 'accepted', 'second-execution', transport=transport)
        self.assertEqual(again, result)
        self.assertTrue(result['published'])
        self.assertEqual(len(transport.calls), 66)
        self.assertEqual(len(set(transport.calls)), 66)
        packet = model.strict(client.data[model.CURRENT])
        self.assertEqual(packet['data_date'], DATES[-1])
        self.assertEqual(packet['squeeze_candidates'], [])
        self.assertTrue(all(packet[k] is False for k in model.FLAGS))
        writes = [v for v in client.writes if v['Key'] == model.CURRENT]
        self.assertEqual(len(writes), 1)
        self.assertIn('IfMatch', writes[0])
        self.assertEqual(writes[0]['CacheControl'], 'no-store')

    def test_error_or_empty_source_preserves_current_and_cannot_recollect_same_id(self):
        for empty in (False, True):
            old = self.previous()
            client = S3({model.CURRENT: old})
            transport = Transport(fail=not empty, empty=empty)
            with patch.object(producer, 'datetime', FixedDateTime):
                with self.assertRaises(ValueError):
                    producer.run(client, 'bucket', 'failed', 'execution', transport=transport)
                count = len(transport.calls)
                with self.assertRaisesRegex(ValueError, 'already attempted'):
                    producer.run(client, 'bucket', 'failed', 'execution', transport=transport)
            self.assertEqual(len(transport.calls), count)
            self.assertEqual(client.data[model.CURRENT], old)
            self.assertEqual(model.strict(client.data[producer.request_key('failed')])['status'], 'failed')
            self.assertTrue(any(v == (b'' if empty else b'unavailable') for k, v in client.data.items() if k.endswith('.bin')))

    def test_competing_publication_and_newer_observation_are_preserved(self):
        client = RaceS3({model.CURRENT: self.previous()})
        with patch.object(producer, 'datetime', FixedDateTime):
            result = producer.run(client, 'bucket', 'race', 'execution', transport=Transport())
        self.assertFalse(result['published'])
        self.assertEqual(client.data[model.CURRENT], b'{"concurrent_writer":true}')
        newer = {'generated_at': '2026-09-25T03:00:00Z', 'data_date': '2026-09-25'}
        self.assertFalse(producer.not_older({'generated_at': '2026-09-25T04:00:00Z', 'data_date': '2026-09-24'}, newer))
        self.assertFalse(producer.not_older(newer, newer))

    def test_unreviewed_url_or_insufficient_budget_never_requests_or_writes(self):
        client, transport = S3({}), Transport()
        for url in ('https://user:secret@www.finra.org' + producer.index.PATH, producer.index.URL + '?token=secret',
                    'https://example.org/market', 'https://cdn.finra.org/equity/regsho/daily/CNMSshvol20260924.txt?key=secret'):
            with self.assertRaises(ValueError):
                producer.fetch(client, 'bucket', 'x', 'url', url, 9999999999, transport)
        with self.assertRaises(ValueError):
            producer.run(client, 'bucket', 'x', 'execution', remaining_seconds=100, transport=transport)
        self.assertEqual(client.writes, [])
        self.assertEqual(transport.calls, [])

    def test_source_revision_is_retained_without_overwriting_prior_original(self):
        client = S3({})
        a = producer.protect(client, 'bucket', b'whole original')
        b = producer.protect(client, 'bucket', b'whole revision')
        self.assertNotEqual(a, b)
        self.assertEqual(client.data[a['key']], b'whole original')
        self.assertEqual(client.data[b['key']], b'whole revision')


if __name__ == '__main__':
    unittest.main(verbosity=2)
