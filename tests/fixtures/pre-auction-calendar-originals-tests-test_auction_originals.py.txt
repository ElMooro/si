"""Complete originals, independent arithmetic, hostile manifests and real handler isolation."""
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path
from contextlib import redirect_stdout
import gzip
import io
import json
import runpy
import sys
import tempfile
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT/'aws/shared'))
import auction_originals as model
import auction_original_store as store

START, END, AT = '2026-09-01', '2026-09-17', '2026-09-17T20:00:00+00:00'


def row(**changes):
    return {'auction_date': END, 'issue_date': '2026-09-21', 'cusip': 'TEST00001',
            'security_type': 'Note', 'security_term': '10-Year', 'inflation_index_security': 'No', 'floating_rate': 'No',
            'high_yield': '4.12', 'median_yield': '4.10', 'low_yield': '4.09', 'bid_to_cover_ratio': '2.4',
            'allocation_pctage': '0', 'primary_dealer_accepted': '2', 'direct_bidder_accepted': '1',
            'indirect_bidder_accepted': '7', 'total_accepted': '10', **changes}


def pages(rows=None, size=2, start=START, end=END, at=AT):
    rows = [row(), row(cusip='TEST00002', bid_to_cover_ratio=None, floating_rate='Yes')] if rows is None else rows
    count = max(1, (len(rows)+size-1)//size)
    return [{'request_url': model.url(start, end, size, i+1), 'acquired_at': at, 'http_status': 200,
             'raw': model.encoded({'data': rows[i*size:(i+1)*size], 'meta': {'total-count': len(rows), 'total-pages': count}})}
            for i in range(count)]


class Conflict(Exception):
    response = {'Error': {'Code': 'PreconditionFailed'}}


class Memory:
    def __init__(self):
        self.objects = {}
        self.reads, self.writes = [], []

    def put_object(self, **request):
        key = request['Key']
        self.writes.append(key)
        if request.get('IfNoneMatch') == '*' and key in self.objects:
            raise Conflict()
        raw = request['Body']
        if isinstance(raw, str):
            raw = raw.encode()
        self.objects[key] = {'raw': raw, 'metadata': request.get('Metadata', {})}
        return {'ETag': '"fixture"'}

    def get_object(self, **request):
        key = request['Key']
        self.reads.append(key)
        item = self.objects[key]
        return {'Body': io.BytesIO(item['raw']), 'Metadata': item['metadata'], 'ETag': '"fixture"', 'LastModified': datetime.now(timezone.utc)}

    def read(self, key):
        return store.storage_read(self, 'fixture', key)


def rejects(call):
    try:
        call()
    except (ValueError, RuntimeError, KeyError, TypeError):
        return
    raise AssertionError('Malformed source accepted')


def retained():
    client = Memory()
    ref = store.retain(client, 'fixture', pages(), START, END, size=2, generated_at=AT)
    return client, ref


def test_all_raw_rows_and_unscored_instruments_are_retained():
    output = model.build(pages(size=1), START, END, 1, AT)
    assert output['coverage']['observations'] == 2 and output['coverage']['pages'] == 2
    assert output['observations'][1]['instrument_kind'] == 'FRN'
    assert output['observations'][1]['values_decimal']['btc'] is None
    assert [r['source']['page_number'] for r in output['observations']] == [1, 2]
    assert all(output[k] is False for k in ('calls_eligible', 'forecast_eligible', 'sizing_eligible', 'execution_eligible'))
    assert not output['coverage']['historical_publication_vintages_verified']


def test_independent_decimal_values_zero_missing_and_quote_units():
    measured = model.measurements(row())
    values = measured['values_decimal']
    assert values['allocated_at_high_pct'] == '0' and values['bidder_denominator_usd'] == '10'
    assert float(values['primary_dealer_pct']) == 20 and float(values['direct_pct']) == 10 and float(values['indirect_pct']) == 70
    assert float(values['high_minus_median_bp']) == 2 and values['tail_bp'] is values['wi_tail_bp'] is None
    assert float(values['accepted_billions']) == 1e-8
    for key in model.BIDDERS:
        for value in (None, '', False, '-1', 'nan', '1_000'):
            missing = model.measurements(row(**{key: value}))
            assert missing['values_decimal']['primary_dealer_pct'] is None and key in missing['bidder_missing_fields']
    zero = model.measurements(row(**dict.fromkeys(model.BIDDERS, '0')))
    assert zero['bidder_share_status'] == 'zero_denominator'
    assert zero['values_decimal']['bidder_denominator_usd'] == '0'
    bill = model.measurements(row(security_type='Bill', high_discnt_rate='0', high_yield='999'))
    assert bill['values_decimal']['high_rate'] == '0' and bill['quote_basis'] == 'discount_rate_pct'


def test_direct_native_measurements_must_match_independent_reference():
    support = runpy.run_path(str(ROOT/'tests/test_auction_observations.py'))
    compute = support['detector']()['compute_record_metrics']
    rows = [row(), row(primary_dealer_accepted=None), row(security_type='Bill', high_discnt_rate='0'),
            row(floating_rate='Yes', high_discount_margin='0.055'), row(inflation_index_security='Yes')]
    model.assert_native(rows, compute)
    def broken(r):
        out = compute(r)
        out['direct_pct'] = 0
        return out
    rejects(lambda: model.assert_native(rows, broken))


def test_complete_pages_reject_reordering_missing_duplicate_and_changed_totals():
    source = pages(size=1)
    for bad in (list(reversed(source)), source[:1], source+[source[0]]):
        rejects(lambda: model.build(bad, START, END, 1, AT))
    for alteration in ('rows', 'total', 'duplicate', 'date', 'identity'):
        bad = deepcopy(source)
        doc = model.strict_json(bad[1]['raw'])
        if alteration == 'rows': doc['data'] = []
        elif alteration == 'total': doc['meta']['total-count'] = 3
        elif alteration == 'duplicate': doc['data'][0]['cusip'] = 'TEST00001'
        elif alteration == 'date': doc['data'][0]['auction_date'] = '2025-01-01'
        else: doc['data'][0].pop('cusip')
        bad[1]['raw'] = model.encoded(doc)
        rejects(lambda: model.build(bad, START, END, 1, AT))


def test_request_clocks_http_status_and_strict_json_fail_closed():
    for key, value in (('request_url', 'https://example.com/private'), ('http_status', True), ('http_status', 206),
                       ('acquired_at', '2026-09-18T00:00:00Z'), ('acquired_at', '2026-09-17T00:00:00'),
                       ('raw', b'{"data":[],"data":[],"meta":{}}'), ('raw', b'{"n":NaN}'),
                       ('raw', b' '*(model.MAX_PAGE_BYTES+1))):
        bad = pages(); bad[0][key] = value
        rejects(lambda: model.build(bad, START, END, 2, AT))
    bad = pages(size=1);bad[1]['acquired_at'] = '2026-09-17T19:59:00Z'
    rejects(lambda: model.build(bad, START, END, 1, AT))


def test_complete_empty_window_is_explicit_and_replayable():
    client = Memory()
    ref = store.retain(client, 'fixture', pages([]), START, END, size=2, generated_at=AT)
    output = store.replay(ref['manifest'], client.read)
    assert output['coverage']['observations'] == 0 and output['observations'] == []
    assert not output['calls_eligible']


def test_every_original_and_compiler_is_read_back_before_replay_acceptance():
    client, ref = retained()
    output = store.replay(ref['manifest'], client.read)
    assert output['coverage']['observations'] == 2
    manifest = model.strict_json(client.read(ref['manifest']['key']))
    assert len(manifest['compilers']) == 4
    assert all(v['key'] in client.reads for v in manifest['compilers'].values())
    assert all(v['evidence']['key'] in client.reads for v in manifest['pages'])
    assert all(key.startswith((model.PREFIX, 'data/evidence/treasury-auctions/')) for key in client.reads+client.writes)
    assert ref['direct_measurements_replayed'] and not ref['calls_eligible']


def test_immutable_conflict_replays_same_bytes_without_overwrite():
    client, first = retained()
    before = deepcopy(client.objects)
    second = store.retain(client, 'fixture', pages(), START, END, size=2, generated_at=AT)
    assert first == second and before == client.objects


def test_tampered_original_compiler_output_and_manifest_are_rejected():
    for target in ('original', 'compiler', 'output', 'manifest'):
        client, ref = retained()
        manifest = model.strict_json(client.read(ref['manifest']['key']))
        key = {'original': manifest['pages'][0]['evidence']['key'],
               'compiler': manifest['compilers']['auction_originals']['key'],
               'output': manifest['output']['key'], 'manifest': ref['manifest']['key']}[target]
        client.objects[key]['raw'] += b'changed'
        rejects(lambda: store.replay(ref['manifest'], client.read))


def test_changed_request_clock_or_compiler_closure_rejected_even_with_new_manifest_hash():
    for kind in ('request', 'receipt', 'clock', 'compiler', 'output_key'):
        client, ref = retained()
        manifest = model.strict_json(client.read(ref['manifest']['key']))
        if kind == 'request': manifest['pages'][0]['request_url'] = 'https://example.com/'
        elif kind == 'receipt': manifest['pages'][0]['evidence']['key'] = 'private/account.json'
        elif kind == 'clock': manifest['pages'][0]['evidence']['first_received_at'] = '2026-09-18T00:00:00Z'
        elif kind == 'compiler': manifest['compilers'].pop('treasury_instruments')
        else: manifest['output']['key'] = 'data/prospective-outcomes.json'
        raw = model.encoded(manifest);changed = store.reference('runs', raw)
        client.objects[changed['key']] = {'raw': raw, 'metadata': {}}
        rejects(lambda: store.replay(changed, client.read))
        assert 'private/account.json' not in client.reads and 'data/prospective-outcomes.json' not in client.reads


def native_setup():
    support = runpy.run_path(str(ROOT/'tests/deployment/test_auction_output_ownership.py'))
    client = Memory();scope = support['load']('auction-crisis-detector', client)
    handler = scope['lambda_handler'];env = handler.__globals__
    env.update(get_fed_funds_rate=lambda: 4.25, fetch_upcoming_auctions=lambda *a, **k: [],
               compute_cross_signals=lambda: {}, compute_preauction_concession=lambda rows: rows,
               compute_postissue_performance=lambda rows, **kw: rows, load_pd_fails=lambda *a: {})
    return client, handler


def response(rows):
    raw = model.encoded({'data': rows, 'meta': {'total-pages': 1, 'total-count': len(rows)}})
    stream = io.BytesIO(raw);stream.status = 200
    return stream


def test_actual_native_handler_captures_and_replays_before_publishing_pointer():
    client, handler = native_setup()
    today = datetime.now(timezone.utc).date().isoformat()
    with patch('urllib.request.urlopen', return_value=response([row(auction_date=today)])):
        assert handler({}, None)['statusCode'] == 200
    packet = model.strict_json(client.read('data/auction-crisis.json'))
    ref = packet['auction_originals']
    replay = store.replay(ref['manifest'], client.read)
    assert replay['observations'][0]['cusip'] == 'TEST00001'
    assert client.writes.index(ref['manifest']['key']) < client.writes.index('data/auction-crisis.json')
    assert packet['source_capture_verified'] is False and not packet['calls_eligible']
    assert ref['direct_measurements_replayed'] is True


def test_original_store_failure_cannot_replace_native_publication():
    client, handler = native_setup()
    key = 'data/auction-crisis.json';old = b'{"preexisting":"unchanged"}'
    client.objects[key] = {'raw': old, 'metadata': {}}
    def broken(**request):
        raise RuntimeError('synthetic immutable storage failure')
    client.put_object = broken
    with patch('urllib.request.urlopen', return_value=response([row()])):
        rejects(lambda: handler({}, None))
    assert client.objects[key]['raw'] == old and key not in client.reads


def test_native_acquisition_returns_whole_raw_page_journal_only_after_success():
    support = runpy.run_path(str(ROOT/'tests/test_auction_observations.py'))
    fetch = support['detector']()['fetch_fiscal_auctions'];journal = []
    first = support['page']([row()], total=2, pages=2)
    with patch('urllib.request.urlopen', side_effect=[first, OSError('second page unavailable')]):
        rejects(lambda: fetch(START, END, page_size=1, pages_out=journal))
    assert journal == []
    with patch('urllib.request.urlopen', return_value=response([row()])):
        rows = fetch(START, END, pages_out=journal)
    assert len(rows) == len(journal) == 1 and isinstance(journal[0]['raw'], bytes)


def test_offline_cli_replays_complete_retained_files_without_network():
    client, ref = retained()
    cli = runpy.run_path(str(ROOT/'scripts/replay_auction_originals.py'))
    with tempfile.TemporaryDirectory() as directory:
        root = Path(directory)
        for key, item in client.objects.items():
            path = root/key;path.parent.mkdir(parents=True, exist_ok=True);path.write_bytes(item['raw'])
        reference = root/'reference.json';reference.write_bytes(model.encoded(ref['manifest']))
        output = io.StringIO()
        with redirect_stdout(output), patch('urllib.request.urlopen', side_effect=AssertionError('No network')):
            cli['main'](['--root', str(root), '--reference', str(reference)])
        result = json.loads(output.getvalue())
        assert result['status'] == 'exact_retained_originals_replayed' and result['coverage']['observations'] == 2
        assert not result['calls_eligible'] and not result['sizing_eligible']


def test_maximum_page_count_keeps_all_rows_and_native_arithmetic():
    rows = [row(cusip=f'TEST{i:05d}') for i in range(6000)]
    source = pages(rows, size=200)
    result = model.build(source, START, END, 200, AT)
    assert result['coverage']['pages'] == 30 and len(result['observations']) == 6000
    assert result['observations'][-1]['source']['row_index'] == 199
    assert len(model.encoded(result)) < store.MAX_ARTIFACT
    assert not result['coverage']['provider_snapshot_atomicity_verified']


def test_capture_receipt_corruption_or_readback_failure_prevents_acceptance():
    original_capture = store.evidence_store.capture
    for change in ('clock', 'request', 'hash'):
        client = Memory()
        def corrupted(*args, **kwargs):
            ref = original_capture(*args, **kwargs)
            if change == 'clock':ref['first_received_at'] = '2026-09-18T00:00:00Z'
            elif change == 'request':ref['source_url'] = 'https://example.com/'
            else:ref['sha256'] = '0'*64
            return ref
        with patch.object(store.evidence_store, 'capture', side_effect=corrupted):
            rejects(lambda: store.retain(client, 'fixture', pages(), START, END, size=2, generated_at=AT))
        assert all(key.startswith((model.PREFIX, 'data/evidence/treasury-auctions/')) for key in client.writes)


if __name__ == '__main__':
    tests = [value for key, value in list(globals().items()) if key.startswith('test_')]
    for test in tests:
        test()
    print('Auction originals regressions passed:', len(tests))
