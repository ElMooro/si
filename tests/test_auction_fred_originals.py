"""Whole 17-source acquisition/replay, corruption and actual native integration.

All HTTP/S3 objects are synthetic. No provider, current packet or private read.
"""
from pathlib import Path
from copy import deepcopy
from datetime import datetime, timezone, timedelta
from unittest.mock import patch
from urllib.parse import urlsplit, parse_qs
from contextlib import redirect_stdout
import gzip
import io
import json
import runpy
import sys
import tempfile
import time

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT/'aws/shared'))
import auction_fred_originals as model
import auction_fred_store as store
support = runpy.run_path(str(ROOT/'tests/test_auction_originals.py'))
Memory, rejects = support['Memory'], support['rejects']
AT = '2026-09-29T08:00:00+00:00'
DAY = '2026-09-29'


def record(sid, rows=None, at=AT):
    rows = [{'date': DAY, 'value': '4.2'}] if rows is None else rows
    raw = model.encoded({'observations': rows, 'realtime_start': DAY, 'realtime_end': DAY})
    transport = {'contract': 'auction-fred-transport.v1', 'source_url': model.public_url(sid, model.LIMITS[sid]),
                 'request_started_at': at, 'response_received_at': at, 'response_status': 200,
                 'declared_content_length': len(raw), 'body_bytes': len(raw), 'body_sha256': model.sha(raw),
                 'cache_status': 'network', 'cache_age_seconds': 0, 'cache_ttl_seconds': 1800}
    return {'raw': raw, 'frame': {'series_id': sid, 'requested_limit': model.LIMITS[sid], 'read_status': 'received',
            'response': model.strict_json(raw), 'adapter_read_at': at, 'adapter_transport': transport,
            'http_acquisition_time_verified': False}}


def fixture():
    return {sid: record(sid) for sid in model.LIMITS}


def native(records):
    output = model.build(records, DAY, AT)
    return {'fed_funds_rate': output['fed_funds']['value'], 'cross_signals': output['cross_signals'],
            'benchmark_histories': output['benchmark_histories']}


def retained(records=None):
    records = fixture() if records is None else records
    client = Memory()
    ref = store.retain(client, 'fixture', records, DAY, AT, native(records))
    return client, ref


def test_all_seventeen_sources_and_every_raw_row_are_replayed():
    records = fixture()
    records['DGS10'] = record('DGS10', [{'date': DAY, 'value': '0'}, {'date': '2026-09-28', 'value': '.'}])
    client, ref = retained(records)
    output = store.replay(ref['manifest'], client.read)
    assert ref['coverage']['complete_request_set'] and len(ref['coverage']['received_series']) == 17
    assert output['observations']['DGS10']['excluded_rows'] == [1]
    assert output['benchmark_histories']['DGS10'] == {DAY: 0}
    assert output['source_frames']['DGS10']['response']['observations'][1]['value'] == '.'
    assert not ref['whole_auction_model_replayed'] and all(ref[k] is False for k in model.PERMISSIONS)
    assert all(k.startswith((model.PREFIX, 'data/evidence/fred/')) for k in client.reads + client.writes)


def test_missing_failed_and_unrequested_sources_are_different():
    records = fixture(); records.pop('DGS30')
    records['DFF'] = {'frame': {'series_id': 'DFF', 'requested_limit': 5, 'read_status': 'unavailable',
                              'response': None, 'adapter_read_at': None, 'error': 'adapter_response_unavailable_or_invalid'}, 'raw': None}
    client, ref = retained(records); out = store.replay(ref['manifest'], client.read)
    assert ref['status'] == 'partial' and ref['coverage']['unavailable_series'] == ['DFF']
    assert ref['coverage']['not_requested_series'] == ['DGS30'] and out['fed_funds']['value'] is None
    assert out['benchmark_histories']['DGS30'] == {}
    client, ref = retained({})
    assert ref['status'] == 'unavailable' and not ref['original_bytes_replayed']
    assert ref['native_input_selection_replayed'] and len(ref['coverage']['not_requested_series']) == 17


def test_policy_rate_latest_missing_or_stale_cannot_fallback_to_older_number():
    for value in (None, '.', False, True, 'nan', '1e999', '1_000', ''):
        frame = record('DFF', [{'date': DAY, 'value': value}, {'date': '2026-09-28', 'value': '4.2'}])['frame']
        out = model.policy_rate(frame, model.benchmarks.day(DAY))
        assert out['value'] is None and out['reason'] == 'latest_observation_missing'
    frame = record('DFF', [{'date': '2026-09-01', 'value': '4.2'}])['frame']
    assert model.policy_rate(frame, model.benchmarks.day(DAY))['reason'] == 'latest_observation_too_old'
    assert model.policy_rate(record('DFF', [{'date': DAY, 'value': '0'}])['frame'], model.benchmarks.day(DAY))['value'] == 0


def test_duplicate_and_future_benchmark_dates_cannot_overwrite_or_enter_history():
    for second in (DAY, '2100-01-01', 'invalid'):
        frame = record('DGS10', [{'date': DAY, 'value': '4'}, {'date': second, 'value': '8'}])['frame']
        result = model.observations(frame, 'DGS10', model.benchmarks.day(DAY))
        assert result['selected_history'] == {} and len(result['rows']) == 2
        assert result['reason'] == 'invalid_future_or_duplicate_date'


def test_native_selection_mismatch_prevents_any_archive_write():
    records = fixture()
    for key in ('fed_funds_rate', 'cross_signals', 'benchmark_histories'):
        actual = native(records); actual[key] = None; client = Memory()
        rejects(lambda: store.retain(client, 'fixture', records, DAY, AT, actual))
        assert client.writes == []


def test_transport_frame_and_original_must_match_before_writing():
    for field in ('hash', 'url', 'clock', 'parsed', 'raw', 'status', 'limit', 'cache', 'missing_transport'):
        records = fixture(); r = records['DFF']; f = r['frame']; t = f['adapter_transport']
        if field == 'hash': t['body_sha256'] = '0'*64
        elif field == 'url': t['source_url'] += '&api_key=secret'
        elif field == 'clock': t['response_received_at'] = '2100-01-01T00:00:00Z'
        elif field == 'parsed': f['response']['observations'][0]['value'] = '9'
        elif field == 'raw': r['raw'] += b' '
        elif field == 'status': t['response_status'] = True
        elif field == 'limit': f['requested_limit'] = True
        elif field == 'cache': t['cache_age_seconds'] = float('nan')
        else: f.pop('adapter_transport')
        client = Memory(); rejects(lambda: store.retain(client, 'fixture', records, DAY, AT, {}))
        assert client.writes == []


def test_compiler_original_output_and_manifest_corruption_are_rejected():
    for field in ('compiler', 'original', 'output', 'manifest'):
        client, ref = retained(); manifest = model.strict_json(client.read(ref['manifest']['key']))
        key = {'compiler': manifest['compilers']['auction_fred_originals']['key'],
               'original': manifest['sources']['DFF']['evidence']['key'],
               'output': manifest['output']['key'], 'manifest': ref['manifest']['key']}[field]
        client.objects[key]['raw'] += b'corrupt'
        rejects(lambda: store.replay(ref['manifest'], client.read))


def test_rehashed_hostile_references_cannot_read_private_or_unrelated_keys():
    for field in ('original', 'compiler', 'output', 'closure'):
        client, ref = retained(); manifest = model.strict_json(client.read(ref['manifest']['key']))
        if field == 'original': manifest['sources']['DFF']['evidence']['key'] = 'private/account.json'
        elif field == 'compiler': manifest['compilers']['auction_fred_originals']['key'] = 'private/account.json'
        elif field == 'output': manifest['output']['key'] = 'data/prospective-outcomes.json'
        else: manifest['compilers'].pop('auction_originals')
        raw = model.encoded(manifest); changed = store.reference('runs', raw)
        client.objects[changed['key']] = {'raw': raw, 'metadata': {}}
        rejects(lambda: store.replay(changed, client.read))
        assert 'private/account.json' not in client.reads and 'data/prospective-outcomes.json' not in client.reads


def test_bounded_gzip_corruption_and_expansion_cannot_bypass_original_identity():
    client, ref = retained(); manifest = model.strict_json(client.read(ref['manifest']['key']))
    key = manifest['sources']['DFF']['evidence']['key']
    for bad in (b'bad-gzip', gzip.compress(b'x' * (model.MAX_BODY + 1)), client.objects[key]['raw'][:-5]):
        client.objects[key]['raw'] = bad
        rejects(lambda: store.replay(ref['manifest'], client.read))


def test_conditional_conflict_validates_existing_whole_original():
    client, ref = retained(); records = fixture()
    assert store.retain(client, 'fixture', records, DAY, AT, native(records)) == ref
    # Other engines use the existing shared writer at this same content key.
    # Our first write must remain readable by that protocol on a later conflict.
    import evidence_store
    shared = evidence_store.capture(client, 'fixture', 'fred', model.public_url('DFF', 5),
                                    records['DFF']['raw'], model.clock(AT) + timedelta(minutes=5))
    assert shared['first_received_at'] == AT and shared['captured'] is True
    first = next(k for k in client.objects if k.startswith('data/evidence/fred/'))
    client.objects[first]['raw'] = gzip.compress(b'wrong')
    rejects(lambda: store.retain(client, 'fixture', records, DAY, AT, native(records)))


def test_warm_cache_original_clock_is_retained_and_not_a_new_publication_vintage():
    records = fixture()
    for record_ in records.values():
        f = record_['frame']; f['adapter_read_at'] = AT
        f['adapter_transport'].update(request_started_at='2026-09-29T07:55:00+00:00', response_received_at='2026-09-29T07:55:00+00:00',
                                      cache_status='fresh_cache', cache_age_seconds=300)
    client, ref = retained(records); manifest = model.strict_json(client.read(ref['manifest']['key']))
    assert manifest['sources']['DFF']['evidence']['response_received_at'] == '2026-09-29T07:55:00+00:00'
    assert not ref['historical_point_in_time_verified']


def test_actual_native_seventeen_requests_share_exact_retained_inputs():
    engine = runpy.run_path(str(ROOT/'tests/test_auction_cross_observations.py'))['engine']()
    transport = runpy.run_path(str(ROOT/'tests/test_auction_fred_transport.py'))
    calls = []
    today = datetime.now(timezone.utc).date().isoformat()
    def respond(url, **kw):
        query = parse_qs(urlsplit(url).query); sid = query['series_id'][0]
        assert int(query['limit'][0]) == model.LIMITS[sid]; calls.append(sid)
        return transport['Response'](model.encoded({'observations': [{'date': today, 'value': '0'}]}), url=url)
    engine.reset_fred_sources(); engine.reset_yield_cache()
    with transport['installed'](respond):
        fed = engine.fetch_fed_funds_rate(); cross = engine.compute_cross_signals()
        history = engine.fetch_cmt_yield_history(); assert engine.fetch_cmt_yield_history() is history
    records, day, used = engine.retained_fred_inputs(fed, cross)
    assert calls == list(model.LIMITS) and len(calls) == 17
    client = Memory(); ref = store.retain(client, 'fixture', records, day, datetime.fromtimestamp(time.time(), timezone.utc).isoformat(), used)
    assert ref['native_input_selection_replayed'] and ref['fed_funds']['value'] == 0
    assert ref['coverage']['raw_rows'] == 17
    engine.reset_fred_sources(); assert engine._fred_records == {} and engine._fred_occurrences == {}


def test_actual_native_adapters_reject_bool_duplicate_and_keep_full_response():
    engine = runpy.run_path(str(ROOT/'tests/test_auction_cross_observations.py'))['engine']()
    today = datetime.now(timezone.utc).date().isoformat()
    def response(rows):
        stream = io.BytesIO(model.encoded({'observations': rows})); stream.status = 200; return stream
    with patch.object(engine.urllib.request, 'urlopen', return_value=response([{'date': today, 'value': True}])):
        assert engine.fetch_fed_funds_rate() is None
    with patch.object(engine.urllib.request, 'urlopen', return_value=response([{'date': today, 'value': '4'}, {'date': today, 'value': '8'}])):
        assert engine._fred_get_history('DGS10', 90) == []
    assert len(engine._fred_records['DGS10']['frame']['response']['observations']) == 2


def test_native_handler_publishes_archive_pointer_only_after_full_source_replay():
    # Keep actual FRED adapters and actual archive writer. All other source
    # fixtures are the existing full native Treasury-original harness.
    harness = runpy.run_path(str(ROOT/'tests/deployment/test_auction_output_ownership.py'))
    client = Memory(); scope = harness['load']('auction-crisis-detector', client); env = scope['lambda_handler'].__globals__
    engine = env['compute_cross_signals'].__globals__
    transport = runpy.run_path(str(ROOT/'tests/test_auction_fred_transport.py'))
    today = datetime.now(timezone.utc).date().isoformat(); calls = []
    def respond(url, **kw):
        url = url.full_url if hasattr(url, 'full_url') else url
        if '/fred/series/observations?' in url:
            calls.append(parse_qs(urlsplit(url).query)['series_id'][0])
            return transport['Response'](model.encoded({'observations': [{'date': today, 'value': '4'}]}), url=url)
        if 'treasurydirect.gov' in url:
            return transport['Response'](b'[]', url=url)
        return support['response']([support['row'](auction_date=today, issue_date=today)])
    env['load_pd_fails'] = lambda *a: {}
    with redirect_stdout(io.StringIO()), transport['installed'](respond):
        assert scope['lambda_handler']({}, None)['statusCode'] == 200
    packet = model.strict_json(client.read('data/auction-crisis.json'))
    assert len(calls) == 17 and packet['fred_source']['status'] == 'complete'
    assert packet['fred_source']['original_bytes_replayed']
    out = store.replay(packet['fred_source']['manifest'], client.read)
    assert out['cross_signals'] == packet['cross_signals']
    assert out['fed_funds']['value'] == packet['fed_funds_rate']
    with redirect_stdout(io.StringIO()), transport['installed'](respond), patch.dict(env, {
            'retain_fred_originals': lambda *a, **kw: (_ for _ in ()).throw(ValueError('synthetic archive failure'))}):
        assert scope['lambda_handler']({}, None)['statusCode'] == 200
    failed = model.strict_json(client.read('data/auction-crisis.json'))['fred_source']
    assert failed['status'] == 'unavailable' and not failed['original_bytes_replayed'] and 'manifest' not in failed


def test_offline_cli_uses_only_bounded_archive_files_and_reviewed_code():
    client, ref = retained(); cli = runpy.run_path(str(ROOT/'scripts/replay_auction_fred.py'))
    with tempfile.TemporaryDirectory() as directory:
        root = Path(directory)
        for key, item in client.objects.items():
            path = root/key; path.parent.mkdir(parents=True, exist_ok=True); path.write_bytes(item['raw'])
        reference = root/'ref.json'; reference.write_bytes(model.encoded(ref['manifest']))
        output = io.StringIO()
        with redirect_stdout(output), patch('urllib.request.urlopen', side_effect=AssertionError('No HTTP')):
            cli['main'](['--root', str(root), '--reference', str(reference)])
        result = json.loads(output.getvalue())
        assert result['status'] == 'exact_retained_originals_replayed' and len(result['coverage']['received_series']) == 17


if __name__ == '__main__':
    tests = [fn for name, fn in list(globals().items()) if name.startswith('test_')]
    for test in tests: test()
    print('Auction FRED original regressions passed:', len(tests))
