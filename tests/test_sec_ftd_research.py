from pathlib import Path
from io import BytesIO
import copy, sys, unittest, zipfile
ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT / 'aws/shared'), str(ROOT / 'tests')]
import sec_ftd_research_model as model
import sec_ftd_measurements as measures
import sec_ftd_source as source
import sec_ftd_research_store as store
from test_option_flow_store import S3
STAMP = '2026-09-25T08:00:00+00:00'


def archive(rows):
    lines = ['|'.join(source.FIELDS), *('|'.join(row) for row in rows),
             'Trailer record count ' + str(len(rows)),
             'Trailer total quantity of shares ' + str(sum(int(row[3]) for row in rows))]
    out = BytesIO()
    info = zipfile.ZipInfo('reported.txt')
    info.compress_type = zipfile.ZIP_DEFLATED
    with zipfile.ZipFile(out, 'w') as packed:
        packed.writestr(info, ('\n'.join(lines) + '\n').encode())
    return out.getvalue()


def fixture(mutate=None):
    blobs = {}
    def retained(body):
        ref = {'key': model.PRIVATE + model.sha(body) + '.bin', 'sha256': model.sha(body), 'bytes': len(body)}
        blobs[ref['key']] = body
        return ref
    def cap(url, body):
        return {'url': url, 'requested_at': STAMP, 'received_at': STAMP, 'http_status': 200,
                'status': 'response_retained', 'headers': {'content-length': str(len(body))}, 'original': retained(body)}
    urls = ['https://www.sec.gov/files/data/' + ('other/' if month == 5 else '') +
            'fails-deliver-data/cnsfails2026' + f'{month:02d}' + half + '.zip'
            for month in range(8, 2, -1) for half in ('b', 'a')]
    index = cap(source.INDEX, ''.join('<a href="' + u + '">archive</a>' for u in reversed(urls)).encode())
    captures = {}
    for i, url in enumerate(urls):
        year, month, half = source.archive_period(url)
        a, b = (15, 16) if month == 7 and half == 'b' else (17, 18) if half == 'b' else (3, 4)
        first, second = f'{year}{month:02d}{a:02d}', f'{year}{month:02d}{b:02d}'
        rows = [[first, '001234567', 'ABC', '100', 'Example class A', '12.3456'],
                [second, '001234567', 'ABC', '150', 'Example class A', '12.50'],
                [second, '901234567', '' if i == 0 else 'XYZ', '12', 'CNS INELIGIBLE SECURITY' if i == 0 else 'Other class', '.']]
        if mutate:
            mutate(url, rows)
        captures[url] = cap(url, archive(rows))
    manifest = {'contract': 'sec-settlement-complete-sources.v1', 'selected_archives': urls,
                'captures': captures, 'index': index, 'selection_cutoff': STAMP[:10]}
    inputs = {'contract': 'sec-ftd-original-inputs.v1', 'generated_at': STAMP, 'archive_count': 12,
              **{k: manifest[k] for k in ('selected_archives', 'captures', 'index', 'selection_cutoff')},
              'source_manifest': retained(model.encoded(manifest))}
    return blobs, inputs


def record(compiled, cusip='001234567'):
    identity = measures.record_id(cusip)
    return compiled['shards'][identity[:2]]['records'][identity]


class Tests(unittest.TestCase):
    def test_complete_population_keeps_every_row_missing_symbol_and_source_locator(self):
        blobs, inputs = fixture()
        compiled = model.compile_output(inputs, blobs.__getitem__)
        packet = compiled['packet']
        self.assertEqual(packet['counts']['original_rows'], 36)
        self.assertEqual(packet['counts']['missing_reported_symbols'], 1)
        self.assertEqual(packet['counts']['cusips'], 2)
        self.assertEqual(len(packet['sources']), 12)
        self.assertEqual(len(record(compiled)['observations']), 24)
        self.assertEqual(record(compiled)['observations'][0][:2], [0, 2])
        self.assertEqual(packet['settlement_date'], '2026-08-18')
        self.assertIn('2026-07-15', packet['dates'])
        self.assertEqual(record(compiled, '901234567')['observations'][-1][3], '')
        self.assertEqual(packet['decision']['eligible_votes'], 0)
        self.assertEqual(packet['by_ticker'], {})
        self.assertTrue(all(packet[k] is False for k in model.FLAGS))

    def test_missing_adjacent_record_is_a_gap_and_never_an_imputed_zero_balance(self):
        blobs, inputs = fixture()
        compiled = model.compile_output(inputs, blobs.__getitem__)
        row = record(compiled, '901234567')
        self.assertTrue(row['missing_reported_dates'])
        for point in row['observations']:
            self.assertEqual(point[-1], 'prior_cusip_record_not_reported')
            self.assertIsNone(point[-2])
            self.assertIsNone(point[-3])

    def test_source_bytes_input_manifest_and_capture_clocks_are_checked(self):
        for failure in ('bytes', 'manifest', 'clock', 'selection', 'count', 'length'):
            blobs, inputs = fixture()
            cap = inputs['captures'][inputs['selected_archives'][0]]
            if failure == 'bytes':
                blobs[cap['original']['key']] += b'x'
            elif failure == 'manifest':
                inputs['source_manifest']['sha256'] = '0'*64
            elif failure == 'clock':
                inputs['generated_at'] = '2026-09-24T00:00:00+00:00'
            elif failure == 'selection':
                inputs['selected_archives'] = inputs['selected_archives'][:-1]
            elif failure == 'count':
                inputs['archive_count'] = True
            else:
                inputs['index']['headers']['content-length'] = '1'
            with self.subTest(failure=failure), self.assertRaises((ValueError, KeyError)):
                model.compile_output(inputs, blobs.__getitem__)

    def test_overlap_never_silently_selects_one_archive_row(self):
        def mutate(url, rows):
            if url.endswith('202608a.zip'):
                rows[0][0] = '20260817'
        blobs, inputs = fixture(mutate)
        with self.assertRaisesRegex(ValueError, 'Overlapping archive'):
            model.compile_output(inputs, blobs.__getitem__)

    def test_immutable_store_replays_all_originals_and_never_writes_a_native_head(self):
        blobs, inputs = fixture()
        client = S3(blobs)
        compiled = model.compile_output(inputs, store.reader(client, 'test'))
        reference = store.retain(client, 'test', inputs, compiled)
        replayed = store.replay(reference, store.reader(client, 'test'))
        self.assertEqual(compiled, replayed)
        self.assertNotIn(model.CURRENT, client.data)
        self.assertTrue(all(v['Key'].startswith(model.PREFIX) for v in client.writes))
        with self.assertRaises(ValueError):
            store.reader(client, 'test')('data/portfolio-snapshot.json')

    def test_changed_source_compiler_and_record_artifacts_fail_replay(self):
        for kind in ('source', 'compiler', 'record'):
            blobs, inputs = fixture()
            client = S3(blobs)
            compiled = model.compile_output(inputs, blobs.__getitem__)
            ref = store.retain(client, 'test', inputs, compiled)
            run = model.strict(client.data[ref['manifest_key']])
            key = (inputs['captures'][inputs['selected_archives'][0]]['original']['key'] if kind == 'source' else
                   next(iter(run['compilers'].values()))['key'] if kind == 'compiler' else
                   next(iter(compiled['packet']['record_shards'].values()))['key'])
            client.data[key] += b'changed'
            with self.subTest(kind=kind), self.assertRaises(ValueError):
                store.replay(ref, store.reader(client, 'test'))


if __name__ == '__main__':
    unittest.main(verbosity=2)
