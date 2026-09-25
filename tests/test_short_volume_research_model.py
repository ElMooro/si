from pathlib import Path
from datetime import date
from copy import deepcopy
import sys, unittest
ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT / 'aws/shared'), str(ROOT / 'tests')]
import short_volume_research_model as model
from test_short_volume_source_index import html
from test_short_volume_measurements import DATES


def fixture():
    objects = {}
    def capture(raw, url):
        ref = {'key': model.PRIVATE + model.sha(raw) + '.bin', 'sha256': model.sha(raw), 'bytes': len(raw)}
        objects[ref['key']] = raw
        return {'url': url, 'original': ref, 'requested_at': '2026-09-25T01:00:02Z',
                'received_at': '2026-09-25T01:00:03Z', 'http_status': 200,
                'status': 'response_retained', 'headers': {'content-length': str(len(raw))}}
    cutoff = date(2026, 9, 25)
    seed = html(tuple(d.replace('-', '') for d in DATES if d.startswith('2026-09')))
    discovery = capture(seed, model.index.URL)
    discovery.update(requested_at='2026-09-25T01:00:00Z', received_at='2026-09-25T01:00:01Z')
    plan = model.index.month_requests(cutoff, model.index.parse(seed, cutoff))
    captures = {'index:discovery': discovery}
    indexes = []
    for spec in plan:
        raw = html(tuple(d.replace('-', '') for d in DATES if d.startswith(spec['period'])))
        captures['index:' + spec['period']] = capture(raw, spec['url'])
        indexes.append(model.index.parse(raw, cutoff, spec['period']))
    selected = model.index.selected_files(indexes, cutoff)
    for n, row in enumerate(selected):
        stamp = row['observation_date']
        raw = ('Date|Symbol|ShortVolume|ShortExemptVolume|TotalVolume|Market\n'
               + stamp.replace('-', '') + '|AAPL|' + str(n + 1) + '.000001|0.000001|100.000002|B,Q,N\n1\n').encode()
        captures['daily:' + stamp] = capture(raw, row['url'])
    inputs = {'contract': 'short-volume-original-inputs.v1', 'generated_at': '2026-09-25T02:00:00Z',
              'selection_cutoff': cutoff.isoformat(), 'month_plan': plan, 'selected_files': selected,
              'captures': captures}
    return inputs, objects


class Tests(unittest.TestCase):
    def test_whole_originals_compile_every_row_into_referenced_shards(self):
        inputs, objects = fixture()
        before = deepcopy(inputs)
        out = model.compile_output(inputs, objects.__getitem__)
        self.assertEqual(inputs, before)
        self.assertEqual(out, model.compile_output(inputs, objects.__getitem__))
        packet = out['packet']
        self.assertEqual(packet['counts']['source_files'], 61)
        self.assertEqual(packet['counts']['source_rows'], 61)
        record = out['shards'][model.bucket('AAPL')]['records']['AAPL']
        self.assertEqual(record['points'][0], [0, 2, '1.000001', '0.000001', '100.000002', 'B,Q,N'])
        self.assertEqual(packet['point_fields'], list(model.measurements.POINT_FIELDS))
        self.assertEqual(packet['daily_sources'][0]['original'], inputs['captures']['daily:' + DATES[0]]['original'])
        self.assertEqual(packet['decision']['verb'], 'WAIT')
        self.assertTrue(all(packet[key] is False for key in model.FLAGS))
        self.assertFalse(packet['quality']['historical_availability_verified'])
        self.assertEqual(packet['squeeze_candidates'], [])
        for key, shard in out['shards'].items():
            self.assertEqual(packet['record_shards'][key], model.record_identity(shard))

    def test_unavailable_original_truncation_and_wrong_hash_fail(self):
        for mutation in ('missing', 'truncate', 'hash'):
            inputs, objects = fixture()
            ref = inputs['captures']['daily:' + DATES[0]]['original']
            if mutation == 'missing':
                del objects[ref['key']]
            elif mutation == 'truncate':
                objects[ref['key']] = objects[ref['key']][:-5]
            else:
                ref['sha256'] = '0' * 64
            with self.assertRaises((ValueError, KeyError)):
                model.compile_output(inputs, objects.__getitem__)

    def test_date_selection_scope_response_and_clocks_are_bound(self):
        for mutation in ('url', 'status', 'length', 'chronology', 'date', 'subset', 'extra', 'year'):
            inputs, objects = fixture()
            row = inputs['captures']['daily:' + DATES[-1]]
            if mutation == 'url':
                row['url'] = 'https://example.org/fake'
            elif mutation == 'status':
                row['http_status'] = 503
            elif mutation == 'length':
                row['headers']['content-length'] = '1'
            elif mutation == 'chronology':
                row['received_at'] = '2026-09-26T01:00:00Z'
            elif mutation == 'date':
                inputs['selection_cutoff'] = '2026-09-26'
            elif mutation == 'subset':
                inputs['selected_files'] = inputs['selected_files'][1:]
            elif mutation == 'extra':
                inputs['captures']['daily:2026-06-01'] = row
            elif mutation == 'year':
                inputs['month_plan'][0]['url'] = inputs['month_plan'][0]['url'].replace('year%5D=7', 'year%5D=2026')
            with self.assertRaises(ValueError, msg=mutation):
                model.compile_output(inputs, objects.__getitem__)

    def test_zero_row_file_does_not_qualify_as_complete_history(self):
        inputs, objects = fixture()
        row = inputs['captures']['daily:' + DATES[0]]
        raw = b'Date|Symbol|ShortVolume|ShortExemptVolume|TotalVolume|Market\n0\n'
        row['original'] = {'key': model.PRIVATE + model.sha(raw) + '.bin', 'sha256': model.sha(raw), 'bytes': len(raw)}
        objects[row['original']['key']] = raw
        row['headers'] = {}
        with self.assertRaisesRegex(ValueError, 'no observations'):
            model.compile_output(inputs, objects.__getitem__)

    def test_private_accounts_and_mutable_current_are_rejected_before_read(self):
        for path in ('data/trade-tickets.json', model.CURRENT):
            inputs, objects = fixture()
            inputs['captures']['daily:' + DATES[0]]['original']['key'] = path
            reads = []
            def read(key):
                reads.append(key)
                return objects[key]
            with self.assertRaises(ValueError):
                model.compile_output(inputs, read)
            self.assertNotIn(path, reads)

    def test_nonfinite_and_duplicate_json_fields_are_rejected(self):
        for raw in (b'{"x":1,"x":2}', b'{"x":NaN}'):
            with self.assertRaises(ValueError):
                model.strict(raw)


if __name__ == '__main__':
    unittest.main(verbosity=2)
