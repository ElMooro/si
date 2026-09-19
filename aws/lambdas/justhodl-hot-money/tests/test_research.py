from copy import deepcopy
from datetime import datetime, timedelta
import gzip
import hashlib
import io
import json
from pathlib import Path
import sys
import unittest
from unittest.mock import patch
ROOT = Path(__file__).resolve().parents[4]
sys.path[:0] = [str(ROOT/'aws/shared'), str(ROOT/'scripts'), str(Path(__file__).resolve().parents[1]/'source')]
import hot_native as native
import hot_research as model
import hot_store as store
from evidence_store import capture
from replay_hot_money import replay
NOW = '2026-09-19T08:30:00+00:00'
FIXTURES = Path(__file__).parent/'fixtures'


class StorageError(Exception):
    def __init__(self, code): self.response = {'Error': {'Code': code}}


class Storage:
    def __init__(self): self.objects = {}; self.metadata = {}; self.race = None
    def get_object(self, **kw):
        key = kw['Key']
        if key not in self.objects: raise StorageError('NoSuchKey')
        raw = self.objects[key]
        return {'Body': io.BytesIO(raw), 'ETag': hashlib.sha256(raw).hexdigest(), 'Metadata': self.metadata.get(key, {})}
    def put_object(self, **kw):
        key = kw['Key']; raw = kw['Body']
        if self.race and key == store.STATE:
            action = self.race; self.race = None; action(self)
        if kw.get('IfNoneMatch') == '*' and key in self.objects: raise StorageError('PreconditionFailed')
        if kw.get('IfMatch') and (key not in self.objects or hashlib.sha256(self.objects[key]).hexdigest() != kw['IfMatch']): raise StorageError('PreconditionFailed')
        self.objects[key] = raw; self.metadata[key] = kw.get('Metadata', {})


def fixture(board): return json.loads((FIXTURES/({'twse':'twse','tpex':'tpex-history','tpex_openapi':'tpex'}[board]+'.json')).read_text(encoding='utf-8'))


def descriptor(client, board, j=None, requested=None, stamp=NOW):
    raw = model.encoded(j if j is not None else fixture(board)); url = native.requested_url(board, requested)
    ev = capture(client, 'fixture', 'twse' if board == 'twse' else 'tpex', url.split('?')[0], raw, datetime.fromisoformat(stamp))
    d = {'board': board, 'url': url, 'requested_day': requested, 'acquired_at': stamp, 'evidence': ev}
    return d, raw


def point(date, net, board='twse'):
    return {'date': date, 'net_twd': str(net), 'buy_twd': str(max(net,0)), 'sell_twd': str(max(-net,0)),
            'acquired_at': NOW, 'original': {}, 'definition': {}, 'categories': {}, 'notes': [], 'board': board}


def inputs(client):
    state, _ = store.read_state(client, 'fixture'); updates = []
    for board in model.BOARDS:
        d, raw = descriptor(client, board, requested='20260918' if board == 'tpex' else None)
        updates.append((board, '20260918', d))
    state = store.merge(state, updates, {}, {}, NOW); probe, _ = descriptor(client, 'tpex_openapi')
    return {'state': state, 'generated_at': NOW, 'openapi': probe, 'legacy_rows': {}, 'legacy_context': {},
            'fails_context': {}, 'current_errors': {}, 'backfill_errors': {}}


class ExchangeResearch(unittest.TestCase):
    def test_actual_original_categories_units_and_openapi_copy_reconcile(self):
        c = Storage(); i = inputs(c); out = model.build(i, store.originals_for(i['state'], i['openapi'], store.raw_reader(c, 'fixture')))
        tw = out['countries']['taiwan']; self.assertEqual(tw['latest']['net_twd'], '86994361942')
        self.assertEqual(tw['otc']['latest']['net_twd'], '21484732547')
        self.assertEqual(tw['combined']['latest_twd'], '108479094489'); self.assertEqual(out['source_comparisons']['tpex']['status'], 'agree')
        self.assertEqual(tw['latest']['categories']['investors']['original_row_index'], 3)
        self.assertIsNone(out['call']); self.assertFalse(out['calls_eligible']); self.assertFalse(out['sizing_eligible'])

    def test_duplicates_and_category_net_mismatch_rejected(self):
        for board in model.BOARDS:
            j = fixture(board); rows = j['data'] if board == 'twse' else j['tables'][0]['data']; rows.append(deepcopy(rows[3 if board == 'twse' else 0]))
            c = Storage(); d, raw = descriptor(c, board, j, '20260918' if board == 'tpex' else None)
            with self.assertRaisesRegex(ValueError, 'duplicate'): native.parse(board, raw, d)
        j = fixture('tpex'); j['tables'][0]['data'][1][1] = '115553185642'; d, raw = descriptor(Storage(), 'tpex', j, '20260918')
        with self.assertRaisesRegex(ValueError, 'buy minus sell'): native.parse('tpex', raw, d)

    def test_tpex_total_cannot_double_count_components_or_accept_wrong_product(self):
        j = fixture('tpex'); j['tables'][0]['data'][0][1] = '115553185642'; j['tables'][0]['data'][0][3] = '21484732548'
        d, raw = descriptor(Storage(), 'tpex', j, '20260918')
        with self.assertRaisesRegex(ValueError, 'total differs'): native.parse('tpex', raw, d)
        j = fixture('tpex'); j['tables'][0]['prod'] = '0'; d, raw = descriptor(Storage(), 'tpex', j, '20260918')
        with self.assertRaisesRegex(ValueError, 'product'): native.parse('tpex', raw, d)

    def test_units_dates_requests_receipt_and_exact_integer_types(self):
        for value in (True, 1.1, 'NaN', '1e3', '1,23', '--2'):
            with self.assertRaises(ValueError): native.integer(value)
        self.assertEqual(native.integer('-1,234'), -1234)
        for field, value in (('hints','USD'), ('date','20260920')):
            j = fixture('twse'); j[field] = value; d, raw = descriptor(Storage(), 'twse', j)
            with self.assertRaises(ValueError): native.parse('twse', raw, d)
        d, raw = descriptor(Storage(), 'tpex', requested='20260917')
        with self.assertRaisesRegex(ValueError, 'requested'): native.parse('tpex', raw, d)
        d, raw = descriptor(Storage(), 'twse'); d['evidence']['sha256'] = '0'*64
        with self.assertRaises(ValueError): native.parse('twse', raw, d)

    def test_zero_is_valid_stale_or_failed_acquisition_cannot_be_renewed(self):
        rows = {'20260918': point('20260918', 0)}
        self.assertEqual(model.board_view('twse', rows, {}, NOW, False)['latest_bn'], 0)
        self.assertIsNone(model.board_view('twse', rows, {}, NOW, True)['latest_bn'])
        rows['20260918']['acquired_at'] = '2026-09-17T00:00:00Z'
        self.assertEqual(model.board_view('twse', rows, {}, NOW, False)['quality']['status'], 'stale')
        self.assertIsNone(model.board_view('twse', rows, {}, NOW, False)['latest_bn'])

    def test_observation_windows_preserve_known_unverified_holes(self):
        rows = {(datetime(2026,9,18)-timedelta(days=i*2)).strftime('%Y%m%d'): point((datetime(2026,9,18)-timedelta(days=i*2)).strftime('%Y%m%d'), 10**9) for i in range(5)}
        good = model.board_view('twse', rows, {}, NOW, False); self.assertEqual(good['sum_5obs_bn'], 5); self.assertIsNone(good['sum_5d_bn'])
        bad = model.board_view('twse', rows, {'20260917':'1'}, NOW, False)
        self.assertIsNone(bad['sum_5obs_bn']); self.assertEqual(bad['windows']['5']['known_unverified_dates'], ['20260917'])

    def test_z_uses_sixty_prior_rows_and_is_not_clipped(self):
        dates = [(datetime(2026,9,18)-timedelta(days=i)).strftime('%Y%m%d') for i in reversed(range(61))]
        rows = {d: point(d, i%2) for i,d in enumerate(dates)}; rows[dates[-1]] = point(dates[-1], 1000)
        out = model.board_view('twse', rows, {}, NOW, False); self.assertGreater(out['z_60_observations'], 4)
        self.assertEqual(out['z_definition']['sample_size'], 60); self.assertNotIn(dates[-1], out['z_definition']['baseline_dates'])
        for d in dates[:-1]: rows[d] = point(d, 1)
        self.assertIsNone(model.board_view('twse', rows, {}, NOW, False)['z_60_observations'])

    def test_combined_windows_require_sixty_on_each_and_exact_dates(self):
        dates = [(datetime(2026,9,18)-timedelta(days=i)).strftime('%Y%m%d') for i in range(61)]
        rows = {d: point(d, 10**9) for d in dates}; a = model.board_view('twse', rows, {}, NOW, False)
        b = model.board_view('tpex', {d: rows[d] for d in dates[:20]}, {}, NOW, False)
        self.assertIsNone(model.combined_view(a,b)['windows']['5']['sum_bn'])
        b = model.board_view('tpex', rows, {}, NOW, False); self.assertEqual(model.combined_view(a,b)['windows']['60']['sum_bn'], 120)
        b['windows']['5']['dates'][0] = '20260901'; self.assertIsNone(model.combined_view(a,b)['windows']['5']['sum_bn'])
        b['latest_day'] = '20260917'; self.assertEqual(model.combined_view(a,b)['status'], 'MISALIGNED')

    def test_corrections_keep_previous_original_and_older_capture_cannot_replace(self):
        c = Storage(); i = inputs(c); state = i['state']; j = fixture('twse'); j['data'][3][1] = '585247334036'; j['data'][3][3] = '86994361943'
        newer, _ = descriptor(c, 'twse', j, stamp='2026-09-19T09:00:00+00:00')
        merged = store.merge(state, [('twse','20260918',newer)], {}, {}, '2026-09-19T09:00:00+00:00')
        self.assertEqual(len(merged['boards']['twse']['20260918']['revisions']), 1)
        old = state['boards']['twse']['20260918']['descriptor']; again = store.merge(merged, [('twse','20260918',old)], {}, {}, NOW)
        self.assertEqual(again['boards'], merged['boards'])

    def test_store_preserves_legacy_replays_originals_and_rejects_tamper(self):
        c = Storage(); old = b'{"rows":{"20260917":123}}'; c.objects[store.LEDGERS['twse']] = old
        c.objects[store.CURRENT] = b'{"legacy":true}'
        def fetch(client,bucket,board,requested=None):
            d,raw = descriptor(client,board,requested=requested); return d,native.parse(board,raw,d)
        with patch.object(store,'now',return_value=NOW), patch.object(store,'acquire',side_effect=fetch):
            result = store.run(c,'fixture',{'max_backfill_requests':0})
        self.assertTrue(result['published']); out=json.loads(c.objects[store.CURRENT]); manifest=json.loads(c.objects[out['replay']['manifest_key']])
        self.assertEqual(replay(manifest,store.raw_reader(c,'fixture')),{k:v for k,v in out.items() if k!='replay'})
        self.assertEqual(c.objects[out['legacy_context'][store.LEDGERS['twse']]['key']],old)
        projection=json.loads(c.objects[store.LEDGERS['twse']]); self.assertEqual(projection['legacy_unverified_dates'],['20260917'])
        original=out['countries']['taiwan']['latest']['original']['key'];c.objects[original]=gzip.compress(b'{}')
        with self.assertRaises(ValueError): replay(manifest,store.raw_reader(c,'fixture'))

    def test_ledger_read_denial_fails_closed_and_current_cannot_regress(self):
        c=Storage()
        with patch.object(c,'get_object',side_effect=StorageError('AccessDenied')):
            with self.assertRaises(StorageError):store.run(c,'fixture',{'max_backfill_requests':0})
        self.assertEqual(c.objects,{})
        packet={'contract':model.CONTRACT,'generated_at':NOW,'source_generated_at':NOW};c.objects[store.CURRENT]=model.encoded(packet)
        self.assertFalse(store.publish(c,'fixture',store.CURRENT,{**packet,'generated_at':'2026-09-18T00:00:00Z'}))
        self.assertFalse(store.publish(c,'fixture',store.CURRENT,{**packet,'generated_at':'2026-09-19T10:00:00Z','source_generated_at':'2026-09-18T00:00:00Z'}))

    def test_concurrent_ledger_update_is_merged_without_losing_other_dates(self):
        c=Storage();j=fixture('twse');j['date']='20260917';j['params']['dayDate']='20260917'
        d,_=descriptor(c,'twse',j,'20260917');state,_=store.read_state(c,'fixture')
        other=store.merge(state,[('twse','20260917',d)],{},{},NOW)
        c.race=lambda storage:storage.objects.update({store.STATE:model.encoded(other)})
        def fetch(client,bucket,board,requested=None):
            desc,raw=descriptor(client,board,requested=requested);return desc,native.parse(board,raw,desc)
        with patch.object(store,'now',return_value=NOW),patch.object(store,'acquire',side_effect=fetch):
            result=store.run(c,'fixture',{'max_backfill_requests':0})
        self.assertTrue(result['published']);state=json.loads(c.objects[store.STATE])
        self.assertEqual(set(state['boards']['twse']),{'20260917','20260918'})

    def test_failed_current_acquisition_retains_history_but_cannot_claim_fresh(self):
        c=Storage();i=inputs(c);c.objects[store.STATE]=model.encoded(i['state'])
        with patch.object(store,'now',return_value=NOW),patch.object(store,'acquire',side_effect=TimeoutError('source unavailable')):
            result=store.run(c,'fixture',{'max_backfill_requests':0})
        self.assertTrue(result['published']);out=json.loads(c.objects[store.CURRENT])
        self.assertEqual(out['quality']['fresh_boards'],0);self.assertIsNone(out['countries']['taiwan']['latest_bn'])
        self.assertEqual(out['countries']['taiwan']['history'][0]['net_twd'],'86994361942')

    def test_scope_units_and_source_clock_are_required_for_fails_context(self):
        d={'generated_at':NOW,'headline':{'as_of':'2026-09-09','ftd_bn':86,'ftr_bn':87,'combined_bn':173,
            'field_units':{'ftd_bn':'usd_bn','ftr_bn':'usd_bn','combined_bn':'usd_bn'}}}
        result=model.fails_context(d,NOW);self.assertIsNone(result['combined_bn']);self.assertEqual(result['ust_ex_tips']['combined_bn'],173)
        self.assertFalse(result['ust_ex_tips']['original_provider_verified'])
        d['headline']['field_units']['ftd_bn']='twd_bn';self.assertIsNone(model.fails_context(d,NOW)['ust_ex_tips']['ftd_bn'])

    def test_handler_routes_only_original_research(self):
        import importlib.util,types
        with patch.dict(sys.modules,{'boto3':types.SimpleNamespace(client=lambda *a,**k:None)}):
            spec=importlib.util.spec_from_file_location('hot_route',ROOT/'aws/lambdas/justhodl-hot-money/source/lambda_function.py')
            engine=importlib.util.module_from_spec(spec);spec.loader.exec_module(engine)
        with patch.object(store,'run',return_value={'published':True}) as active,patch.object(engine,'_legacy_unvalidated_handler',side_effect=AssertionError('legacy route')):
            self.assertEqual(engine.lambda_handler({'action':'legacy'},None)['statusCode'],200);active.assert_called_once()


if __name__ == '__main__': unittest.main()
