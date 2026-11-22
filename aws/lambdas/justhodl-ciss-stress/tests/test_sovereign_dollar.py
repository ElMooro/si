"""The dollar-shortage document: the desk's three ECB auxiliaries again, from the same retained responses, with whole histories,
beside the Federal Reserve's dollar series as the site's warehouse serves them."""
from datetime import datetime, timedelta
from decimal import Decimal
import copy
import json
import sys
import time
import urllib.parse
import unittest
from pathlib import Path

HERE = Path(__file__).resolve()
sys.path[:0] = [str(HERE.parents[1] / 'source'), str(HERE.parents[3] / 'shared'), str(HERE.parent)]

import ciss_source_model as model          # noqa: E402
import ciss_source_store as store          # noqa: E402
import sovereign_desk as desk              # noqa: E402
import dollar_shortage as dollar           # noqa: E402
import test_sovereign_desk as base         # noqa: E402

LATER = '2026-11-22T18:55:00+00:00'
ECB = ['AUX_EURUSD', 'AUX_FX_EA', 'AUX_FX_NONEA']
LEGS = {'DEXUSEU': [1.0600, 1.0550, 1.0500], 'DEXJPUS': [155.10, 156.20, 157.30], 'DEXUSUK': [1.2700, 1.2650, 1.2600],
        'DEXCAUS': [1.4100, 1.4200, 1.4256], 'DEXSDUS': [10.90, 10.95, 11.00], 'DEXSZUS': [0.8900, None, 0.9000]}
FED = {
    'SWPT': [['2026-11-04', 150.0], ['2026-11-11', 120.0], ['2026-11-18', 146.0]],                          # USD millions
    'H41RESPPALGTRFNWW': [['2026-11-11', 0.0], ['2026-11-18', 1.0]],
    'DTWEXBGS': [['2026-11-12', 122.1], ['2026-11-13', 122.6503]],
    'DTWEXAFEGS': [['2026-11-12', 116.0], ['2026-11-13', 116.236]],
    'DTWEXEMEGS': [['2026-11-12', 130.5], ['2026-11-13', 130.8877]],
    'WMTSECL1': [['2026-11-11', 2550000.0], ['2026-11-18', 2544011.0]],
    'WSEFINTL1': [['2026-11-11', 2820000.0], ['2026-11-18', 2813231.0]],
    'WLRRAFOIAL': [['2026-11-11', 310000.0], ['2026-11-18', 316712.0]],
    'SOFR': [['2021-07-27', 0.05], ['2021-07-28', 0.05], ['2021-07-29', 0.05], ['2026-11-19', 3.91]],
    'IOER': [['2021-07-27', 0.15], ['2021-07-28', 0.15], ['2021-07-29', 9.99]],                             # a value after its last day is not used
    'IORB': [['2021-07-28', 9.99], ['2021-07-29', 0.15], ['2026-11-19', 3.90], ['2026-11-23', 3.90]],        # nor one before its first; tomorrow is announced ahead
    'TEDRATE': [['1998-12-31', 0.50], ['2008-10-10', 4.58], ['2022-01-21', 0.09]],                           # percent; ended
    'NDFFRIW027NBOG': [['1998-12-30', 10.0], ['2008-05-28', -615.423], ['2026-11-11', 885.647]],             # USD billions
}
for _sid, _values in LEGS.items():
    FED[_sid] = [[day, v] for day, v in zip(('2026-11-11', '2026-11-12', '2026-11-13'), _values) if v is not None]


def published():
    """A bucket holding the originals and the desk document compiled from them; (s3, desk document, read)."""
    s3, binding, receipts, aux = base.staged()
    read = lambda receipt: store.original(s3, 'test', receipt)                                  # noqa: E731
    doc, _ = desk.compile_desk(binding, receipts, aux, read, base.NOW, {})
    doc['replay'] = {'manifest_key': desk.RUNS + 'abc.json', 'output_sha256': 'f' * 64}
    s3.objects[desk.CURRENT] = model.encoded(doc)
    return s3, doc, read


def resolver(table=None, calls=None, refuse=()):
    """The site's series resolver, from a table of FRED ids; (payload, raw bytes) as world_history.http_json returns them."""
    table = FED if table is None else table

    def fetch(url):
        sid = urllib.parse.unquote(url.split('?id=')[1])
        if calls is not None:
            calls.append(sid)
        if sid in refuse or sid[5:] not in table:
            raise OSError('refused')
        payload = {'obs': table[sid[5:]], 'source': 'warehouse:test'}
        return payload, json.dumps(payload).encode()
    return fetch


def kept(table=None, at=LATER):
    state = {'contract': dollar.STATE_CONTRACT, 'fred': {}}
    errors, asked = dollar.refresh(state, datetime.fromisoformat(at), resolver(table), time.monotonic() + 60, sleep=lambda s: None)
    assert not errors and asked == len(dollar.FRED_IDS), errors
    return state['fred']


class Compile(unittest.TestCase):
    def test_the_three_measures_are_the_desks_own_with_every_observation(self):
        s3, doc, read = published()
        out = dollar.compile_dollar(doc, read, LATER)
        self.assertEqual((out['contract'], out['version'], out['generated_at']), ('ciss-dollar-shortage.v1', dollar.VERSION, LATER))
        self.assertEqual(out['order'], ['AUX_FX_EA', 'AUX_EURUSD', 'AUX_FX_NONEA'])                 # lending, then the price, then the reserves
        self.assertEqual(out['ecb'], ECB)
        self.assertEqual([(p['id'], p['ids']) for p in out['parts']], [('backstop', ['AUX_FX_EA']), ('price', ['AUX_EURUSD']), ('reserves', ['AUX_FX_NONEA'])])
        self.assertEqual(set(out['measures']), set(out['series']))
        for mid in ECB:
            m, s, d = out['measures'][mid], out['series'][mid], doc['measures'][mid]
            for field in ('value', 'date', 'period', 'exact', 'n', 'start', 'high', 'low', 'change', 'pctile', 'peaks', 'key', 'evidence', 'source_row', 'unit', 'freq', 'label'):
                self.assertEqual(m[field], d[field], mid + '.' + field)
            self.assertEqual((len(s['d']), s['d'][0], s['d'][-1], s['v'][-1]), (m['n'], m['start'], m['date'], m['value']))
            self.assertEqual(s['d'], sorted(set(s['d'])))
            self.assertTrue(m['what'] and m['short'])
        self.assertEqual(out['measures']['AUX_EURUSD']['value'], 1.099)
        self.assertEqual(out['series']['AUX_EURUSD']['v'][-2:], [1.0992, 1.099])
        self.assertEqual(out['measures']['AUX_FX_EA']['value'], 20.193)                         # EUR millions at the ECB, billions here
        self.assertEqual(out['measures']['AUX_FX_NONEA']['value'], 521.892)
        self.assertEqual([(out['measures'][k]['worse'], out['measures'][k]['part'], out['measures'][k]['source']) for k in ECB],
                         [('lower', 'price', 'ecb'), ('higher', 'backstop', 'ecb'), (None, 'reserves', 'ecb')])
        self.assertIn('IMF', out['measures']['AUX_FX_NONEA']['what'])                              # the reserves item is described as what it is
        self.assertEqual(out['measures']['AUX_FX_NONEA']['short'], 'Eurosystem foreign reserves other than gold')
        self.assertEqual(out['desk'], {'key': desk.CURRENT, 'generated_at': base.NOW, 'version': desk.VERSION, 'replay': doc['replay']})
        self.assertEqual(out['episodes'], desk.EPISODES)
        self.assertEqual(out['coverage'], {'measures': 3, 'observations': sum(doc['measures'][k]['n'] for k in ECB), 'ecb': 3, 'fed': 0, 'errors': 12, 'status': 'degraded'})
        self.assertEqual(sorted(out['errors']), sorted(spec['id'] for spec in dollar.US))           # each Federal Reserve measure is named as not held
        self.assertTrue(out['errors']['USD_DXY'].startswith('NOT_HELD: DEXUSEU, DEXJPUS'))
        self.assertEqual((out['calls_eligible'], out['sizing_eligible']), (False, False))

    def test_the_reference_rate_is_kept_for_every_day_where_the_desks_history_keeps_one_a_week(self):
        s3, doc, read = published()
        out = dollar.compile_dollar(doc, read, LATER)
        days = out['series']['AUX_EURUSD']['d']
        self.assertEqual(len(days), len(base.business_days('2026-10-01', '2026-11-20')))
        self.assertEqual(days[:3], ['2026-10-01', '2026-10-02', '2026-10-05'])                  # consecutive working days
        weekly = desk.weekly(list(zip(days, out['series']['AUX_EURUSD']['v'])))
        self.assertLess(len(weekly), len(days) / 4)
        self.assertEqual(out['series']['AUX_FX_EA']['d'][-1], '2026-11-13')                     # the Friday of ISO week 46

    def test_each_source_names_the_ecb_response_and_its_receipt(self):
        s3, doc, read = published()
        out = dollar.compile_dollar(doc, read, LATER)
        self.assertEqual([(s['id'], s['name'], s['path'], s['key']) for s in out['sources'] if s['name'] != 'fred'], [
            ('AUX_EURUSD', 'eurusd', 'EXR/D.USD.EUR.SP00.A', 'EXR.D.USD.EUR.SP00.A'),
            ('AUX_FX_EA', 'fx_claims_ea', 'ILM/W.U2.C.A030000.U2.Z06', 'ILM.W.U2.C.A030000.U2.Z06'),
            ('AUX_FX_NONEA', 'fx_claims_nonea', 'ILM/W.U2.C.A020000.U4.Z06', 'ILM.W.U2.C.A020000.U4.Z06')])
        out = dollar.compile_dollar(doc, read, LATER, kept())
        fed = [s for s in out['sources'] if s['name'] == 'fred']
        self.assertEqual([s['id'] for s in fed], [spec['id'] for spec in dollar.US])
        self.assertEqual(fed[0]['read'], [{'series': 'SWPT', 'read_at': LATER, 'via': 'warehouse:test', 'sha256': fed[0]['read'][0]['sha256'], 'bytes': fed[0]['read'][0]['bytes']}])
        self.assertEqual(len(fed[0]['read'][0]['sha256']), 64)
        for s in out['sources'][:3]:
            self.assertEqual(s['evidence'], doc['measures'][s['id']]['evidence'])
            self.assertEqual(store.original(s3, 'test', s['evidence'])[:3], b'KEY')
            self.assertEqual((s['observations'], s['carried'], s['acquired_at']), (doc['measures'][s['id']]['n'], False, base.NOW))

    def test_the_same_inputs_give_the_same_bytes_and_the_desk_document_is_not_changed(self):
        s3, doc, read = published()
        before = copy.deepcopy(doc)
        self.assertEqual(model.digest(dollar.compile_dollar(doc, read, LATER)), model.digest(dollar.compile_dollar(copy.deepcopy(doc), read, LATER)))
        held = kept()
        again = copy.deepcopy(held)
        self.assertEqual(model.digest(dollar.compile_dollar(doc, read, LATER, held, {'x': 'y'})), model.digest(dollar.compile_dollar(copy.deepcopy(doc), read, LATER, again, {'x': 'y'})))
        self.assertEqual((doc, held), (before, again))

    def test_a_figure_that_does_not_reproduce_the_desks_is_refused(self):
        s3, doc, read = published()
        for field, wrong in (('value', 1.2), ('date', '2026-11-19'), ('n', 5), ('source_row', 1)):
            bad = copy.deepcopy(doc)
            bad['measures']['AUX_EURUSD'][field] = wrong
            with self.assertRaisesRegex(ValueError, 'AUX_EURUSD does not reproduce the desk document'):
                dollar.compile_dollar(bad, read, LATER)
        gone = copy.deepcopy(doc)
        del gone['measures']['AUX_FX_EA']
        with self.assertRaisesRegex(ValueError, 'does not carry AUX_FX_EA'):
            dollar.compile_dollar(gone, read, LATER)
        other = copy.deepcopy(doc)
        other['measures']['AUX_FX_EA']['evidence'] = doc['measures']['AUX_EURUSD']['evidence']       # a receipt for another response
        with self.assertRaisesRegex(ValueError, 'does not hold ILM.W.U2.C.A030000.U2.Z06'):
            dollar.compile_dollar(other, read, LATER)

    def test_an_observation_newer_than_the_desks_own_day_is_left_for_the_next_desk_document(self):
        s3, doc, read = published()
        older = copy.deepcopy(doc)
        older['generated_at'] = '2026-11-19T07:20:00+00:00'                                     # the desk as it was compiled on the 19th
        m = older['measures']['AUX_EURUSD']
        m.update({'value': 1.0992, 'date': '2026-11-19', 'n': m['n'] - 1, 'source_row': m['source_row'] - 1})
        out = dollar.compile_dollar(older, read, LATER)
        self.assertEqual(out['series']['AUX_EURUSD']['d'][-1], '2026-11-19')


class FederalReserve(unittest.TestCase):
    """The Federal Reserve's series beside the ECB's: units, the rebuilt DXY, the chained reserve rate, what is left out."""

    def compiled(self, table=None):
        s3, doc, read = published()
        return dollar.compile_dollar(doc, read, LATER, kept(table)), doc

    def test_the_parts_list_every_measure_once_in_reading_order(self):
        out, _ = self.compiled()
        self.assertEqual([(p['id'], p['ids']) for p in out['parts']], [
            ('backstop', ['FED_SWAPS', 'FED_FIMA', 'AUX_FX_EA']),
            ('price', ['USD_DXY', 'USD_BROAD', 'USD_AFE', 'USD_EME', 'AUX_EURUSD']),
            ('official', ['FED_CUSTODY_UST', 'FED_CUSTODY_ALL', 'FED_FOREIGN_RRP']),
            ('funding', ['USD_SOFR_IOR', 'USD_TED', 'H8_NET_DUE']),
            ('reserves', ['AUX_FX_NONEA'])])
        self.assertEqual(out['order'], [mid for p in out['parts'] for mid in p['ids']])
        self.assertEqual(sorted(out['order']), sorted(out['measures']))
        self.assertEqual(set(out['measures']), set(out['series']))
        self.assertTrue(all(p['name'] and p['note'] for p in out['parts']))
        self.assertEqual((out['errors'], out['coverage']['status'], out['coverage']['fed'], out['coverage']['ecb']), ({}, 'measured', 12, 3))
        self.assertEqual(out['coverage']['observations'], sum(len(v['d']) for v in out['series'].values()))
        for mid, m in out['measures'].items():
            s = out['series'][mid]
            self.assertEqual((len(s['d']), s['d'][0], s['d'][-1], s['v'][-1]), (m['n'], m['start'], m['date'], m['value']), mid)
            self.assertEqual(s['d'], sorted(set(s['d'])), mid)
            self.assertTrue(m['what'] and m['short'] and m['label'] and m['part'] and m['source'] in ('ecb', 'fred'), mid)
            self.assertIn(m['worse'], ('higher', 'lower', None))

    def test_the_weekly_balance_sheet_items_are_brought_from_millions_to_billions(self):
        out, _ = self.compiled()
        M = out['measures']
        self.assertEqual((M['FED_SWAPS']['value'], M['FED_SWAPS']['date'], M['FED_SWAPS']['unit'], M['FED_SWAPS']['freq']), (0.146, '2026-11-18', 'usd_bn', 'W'))
        self.assertEqual(out['series']['FED_SWAPS'], {'d': ['2026-11-04', '2026-11-11', '2026-11-18'], 'v': [0.15, 0.12, 0.146]})
        self.assertEqual(M['FED_SWAPS']['change']['1w'], 0.026)
        self.assertEqual((M['FED_FIMA']['value'], M['FED_CUSTODY_UST']['value'], M['FED_CUSTODY_ALL']['value'], M['FED_FOREIGN_RRP']['value']), (0.001, 2544.011, 2813.231, 316.712))
        self.assertEqual((M['FED_CUSTODY_UST']['worse'], M['FED_FOREIGN_RRP']['worse'], M['FED_SWAPS']['worse']), ('lower', None, 'higher'))
        self.assertEqual(M['H8_NET_DUE']['value'], 885.647)                                        # H.8 is in billions already
        self.assertEqual(out['series']['H8_NET_DUE']['d'], ['2008-05-28', '2026-11-11'])           # nothing from before the euro
        self.assertEqual((M['H8_NET_DUE']['low'], M['H8_NET_DUE']['status']), ([-615.423, '2008-05-28'], 'current'))
        self.assertEqual((M['FED_SWAPS']['source'], M['FED_SWAPS']['key'], M['FED_SWAPS']['series'], M['FED_SWAPS']['read_at']), ('fred', 'fred:SWPT', ['SWPT'], LATER))
        self.assertIn('H.4.1', M['FED_SWAPS']['source_label'])
        for gone in ('evidence', 'source_row', 'exact', 'area'):
            self.assertNotIn(gone, M['FED_SWAPS'])

    def test_dxy_is_the_index_formula_on_the_days_all_six_rates_exist(self):
        out, _ = self.compiled()
        s, m = out['series']['USD_DXY'], out['measures']['USD_DXY']
        self.assertEqual(s['d'], ['2026-11-11', '2026-11-13'])                                     # the franc has no rate on the 12th
        want = 50.14348112 * 1.05 ** -0.576 * 157.30 ** 0.136 * 1.26 ** -0.119 * 1.4256 ** 0.091 * 11.0 ** 0.042 * 0.90 ** 0.036
        self.assertEqual(m['value'], round(want, 4))
        self.assertTrue(100 < m['value'] < 115, m['value'])
        self.assertEqual((m['unit'], m['freq'], m['worse'], m['series']), ('idx', 'D', 'higher', ['DEXUSEU', 'DEXJPUS', 'DEXUSUK', 'DEXCAUS', 'DEXSDUS', 'DEXSZUS']))
        self.assertEqual(m['status'], 'current')                                                   # nine days old: H.10 comes a week in arrears
        self.assertEqual(out['measures']['USD_BROAD']['value'], 122.6503)
        self.assertEqual(dollar.dxy_obs({sid: [] if sid == 'DEXSZUS' else [('2026-11-13', Decimal(1))] for sid, _ in dollar.DXY_LEGS}), [])
        self.assertEqual(sum(power for _, power in dollar.DXY_LEGS if power > 0) - sum(power for _, power in dollar.DXY_LEGS if power < 0), 1.0)

    def test_sofr_is_set_against_ioer_until_iorb_began_and_iorb_since(self):
        out, _ = self.compiled()
        s, m = out['series']['USD_SOFR_IOR'], out['measures']['USD_SOFR_IOR']
        self.assertEqual(s, {'d': ['2021-07-27', '2021-07-28', '2021-07-29', '2026-11-19'], 'v': [-10.0, -10.0, -10.0, 1.0]})
        self.assertEqual((m['value'], m['unit'], m['key']), (1.0, 'bp', 'fred:SOFR, fred:IORB, fred:IOER'))
        self.assertEqual(dollar.spread_obs([('2021-07-28', Decimal('0.05'))], [('2021-07-28', Decimal('9'))], []), [])   # no IOER for the day: no figure, never IORB's

    def test_the_ted_spread_is_in_basis_points_and_marked_ended(self):
        out, _ = self.compiled()
        m = out['measures']['USD_TED']
        self.assertEqual(out['series']['USD_TED'], {'d': ['2008-10-10', '2022-01-21'], 'v': [458.0, 9.0]})
        self.assertEqual((m['status'], m['high'], m['value'], m['date']), ('ended', [458.0, '2008-10-10'], 9.0, '2022-01-21'))
        self.assertEqual(m['peaks']['gfc2008']['hi'], [458.0, '2008-10-10'])

    def test_a_series_that_is_not_held_leaves_its_measure_out_and_is_named(self):
        table = {k: v for k, v in FED.items() if k not in ('DEXSZUS', 'SWPT')}
        s3, doc, read = published()
        state = {'contract': dollar.STATE_CONTRACT, 'fred': {}}
        errors, asked = dollar.refresh(state, datetime.fromisoformat(LATER), resolver(table), time.monotonic() + 60, sleep=lambda s: None)
        self.assertEqual((errors, asked), ({'fred:DEXSZUS': 'OSError', 'fred:SWPT': 'OSError'}, len(dollar.FRED_IDS)))
        out = dollar.compile_dollar(doc, read, LATER, state['fred'], errors)
        self.assertNotIn('USD_DXY', out['measures'])
        self.assertNotIn('FED_SWAPS', out['measures'])
        self.assertEqual(out['parts'][0]['ids'], ['FED_FIMA', 'AUX_FX_EA'])
        self.assertEqual(out['errors'], {'fred:DEXSZUS': 'OSError', 'fred:SWPT': 'OSError', 'USD_DXY': 'NOT_HELD: DEXSZUS', 'FED_SWAPS': 'NOT_HELD: SWPT'})
        self.assertEqual((out['coverage']['status'], out['coverage']['fed'], out['coverage']['errors']), ('degraded', 10, 4))
        for mid in ECB:                                                                            # the ECB's three are untouched by it
            self.assertEqual(out['measures'][mid]['value'], doc['measures'][mid]['value'])


class Refresh(unittest.TestCase):
    def test_each_series_is_asked_for_once_with_a_pause_between(self):
        calls, pauses, state = [], [], {'contract': dollar.STATE_CONTRACT, 'fred': {}}
        errors, asked = dollar.refresh(state, datetime.fromisoformat(LATER), resolver(calls=calls), time.monotonic() + 60, sleep=pauses.append)
        self.assertEqual(calls, ['fred:' + sid for sid in dollar.FRED_IDS])
        self.assertEqual((len(set(calls)), asked, errors), (19, 19, {}))
        self.assertEqual(pauses, [0.5] * 18)
        self.assertEqual(state['fred']['SWPT']['obs'], [['2026-11-04', '150.0'], ['2026-11-11', '120.0'], ['2026-11-18', '146.0']])
        self.assertEqual(state['fred']['TEDRATE']['obs'][0][0], '2008-10-10')                      # nothing from before 1999 is kept
        self.assertEqual(sorted(state['fred']['SWPT']), ['bytes', 'obs', 'read_at', 'sha256', 'via'])

    def test_nothing_is_asked_again_within_the_day_and_an_ended_series_once_a_week(self):
        state = {'contract': dollar.STATE_CONTRACT, 'fred': kept()}
        start = datetime.fromisoformat(LATER)
        for hours, want in ((12, []), (21, [sid for sid in dollar.FRED_IDS if sid not in ('IOER', 'TEDRATE')])):
            calls, fresh = [], copy.deepcopy(state)
            errors, asked = dollar.refresh(fresh, start + timedelta(hours=hours), resolver(calls=calls), time.monotonic() + 60, sleep=lambda s: None)
            self.assertEqual((calls, asked, errors), (['fred:' + sid for sid in want], len(want), {}), hours)
        table = dict(FED, IOER=[['2021-07-27', 0.15], ['2021-07-28', 0.15]])                        # as the source leaves it: nothing after its last day
        state = {'contract': dollar.STATE_CONTRACT, 'fred': kept(table)}
        calls = []
        dollar.refresh(state, start + timedelta(days=8), resolver(table, calls=calls), time.monotonic() + 60, sleep=lambda s: None)
        self.assertEqual(len(calls), 19)                                                           # a week on, the ended two are looked at again
        self.assertTrue(dollar.due(None, start) and dollar.due({'obs': [], 'read_at': LATER}, start))
        self.assertFalse(dollar.due({'obs': [['2026-11-18', '1']], 'read_at': LATER}, start + timedelta(hours=19)))

    def test_a_refused_series_keeps_what_was_read_before(self):
        state = {'contract': dollar.STATE_CONTRACT, 'fred': kept()}
        before = copy.deepcopy(state['fred']['SOFR'])
        errors, asked = dollar.refresh(state, datetime.fromisoformat(LATER) + timedelta(hours=24), resolver(refuse=('fred:SOFR',)), time.monotonic() + 60, sleep=lambda s: None)
        self.assertEqual(errors, {'fred:SOFR': 'OSError - the series read on 2026-11-22 is used'})
        self.assertEqual(state['fred']['SOFR'], before)
        self.assertEqual(state['fred']['SWPT']['read_at'], '2026-11-23T18:55:00+00:00')
        empty = {'contract': dollar.STATE_CONTRACT, 'fred': {}}
        errors, _ = dollar.refresh(empty, datetime.fromisoformat(LATER), resolver({'SWPT': [['2026-11-18', 146.0]]}), time.monotonic() + 60, sleep=lambda s: None)
        self.assertEqual(errors['fred:SWPT'], 'ValueError')                                        # one observation is not a series
        self.assertEqual(empty['fred'], {})

    def test_out_of_time_stops_asking_and_says_so(self):
        calls, state = [], {'contract': dollar.STATE_CONTRACT, 'fred': {}}
        errors, asked = dollar.refresh(state, datetime.fromisoformat(LATER), resolver(calls=calls), time.monotonic() - 1, sleep=lambda s: None)
        self.assertEqual((calls, asked, state['fred']), ([], 0, {}))
        self.assertTrue(errors['fred'].startswith('OUT_OF_TIME'))


class Publication(unittest.TestCase):
    def run_it(self, s3, fetch=None, at=LATER):
        return dollar.run(s3, 'test', store, now=datetime.fromisoformat(at), fetch=fetch or resolver(), sleep=lambda s: None)

    def test_run_keeps_the_series_it_read_and_then_writes_the_document(self):
        s3, doc, _ = published()
        s3.puts.clear()
        out = self.run_it(s3)
        self.assertEqual(s3.puts, [dollar.STATE, dollar.CURRENT])
        self.assertEqual((out['published'], out['generated_at'], out['desk_generated_at'], out['series_asked']), (True, LATER, base.NOW, 19))
        self.assertEqual(out['coverage']['measures'], 15)
        self.assertEqual(out['latest']['AUX_EURUSD'], [1.099, '2026-11-20'])
        self.assertEqual(out['latest']['FED_SWAPS'], [0.146, '2026-11-18'])
        self.assertEqual(list(out['latest'])[:3], ['FED_SWAPS', 'FED_FIMA', 'AUX_FX_EA'])
        written = json.loads(s3.objects[dollar.CURRENT])
        sha = written.pop('sha256')
        self.assertEqual(sha, model.digest(written))
        state = json.loads(s3.objects[dollar.STATE])
        self.assertEqual((state['contract'], state['updated_at'], sorted(state['fred'])), (dollar.STATE_CONTRACT, LATER, sorted(dollar.FRED_IDS)))
        self.assertEqual(written, dollar.compile_dollar(doc, lambda r: store.original(s3, 'test', r), LATER, state['fred'], {}))
        self.assertTrue(dollar.STATE.startswith('data/ciss-research/'))                            # beside the engine's other records, not among the pages' documents

    def test_a_second_run_in_the_day_asks_for_nothing_and_writes_only_the_document(self):
        s3, doc, _ = published()
        self.run_it(s3)
        s3.puts.clear()
        calls = []
        out = self.run_it(s3, resolver(calls=calls), at='2026-11-23T07:10:00+00:00')
        self.assertEqual((calls, out['series_asked'], s3.puts), ([], 0, [dollar.CURRENT]))
        self.assertEqual(out['coverage']['measures'], 15)

    def test_the_ecb_part_is_published_when_the_warehouse_cannot_be_reached(self):
        s3, doc, _ = published()
        s3.puts.clear()

        def down(url):
            raise OSError('unreachable')
        out = self.run_it(s3, down)
        self.assertEqual((out['published'], out['coverage']['measures'], out['coverage']['status'], out['coverage']['fed']), (True, 3, 'degraded', 0))
        self.assertEqual(list(out['latest']), ['AUX_FX_EA', 'AUX_EURUSD', 'AUX_FX_NONEA'])
        self.assertEqual(s3.puts[-1], dollar.CURRENT)
        written = json.loads(s3.objects[dollar.CURRENT])
        self.assertEqual(written['errors']['fred:SWPT'], 'OSError')
        self.assertEqual(written['errors']['FED_SWAPS'], 'NOT_HELD: SWPT')
        s3.objects[dollar.STATE] = b'not json'                                                     # a state that cannot be read: everything is asked for again
        out = self.run_it(s3)
        self.assertEqual((out['series_asked'], out['coverage']['measures']), (19, 15))
        self.assertIn('UNREADABLE', json.loads(s3.objects[dollar.CURRENT])['errors']['state'])
        s3.objects[dollar.STATE] = model.encoded({'contract': 'something-else', 'fred': {}})
        self.assertEqual(self.run_it(s3)['series_asked'], 19)

    def test_nothing_is_written_without_the_desk_document_or_when_a_figure_differs(self):
        empty = base.MemoryS3()
        with self.assertRaises(base.StorageError):
            self.run_it(empty)
        s3, doc, _ = published()
        s3.objects[desk.CURRENT] = model.encoded({**doc, 'contract': 'something-else'})
        s3.puts.clear()
        with self.assertRaisesRegex(ValueError, 'sovereign desk document required'):
            self.run_it(s3)
        self.assertEqual(s3.puts, [])
        bad = copy.deepcopy(doc)
        bad['measures']['AUX_FX_NONEA']['value'] = 1.0
        s3.objects[desk.CURRENT] = model.encoded(bad)
        with self.assertRaisesRegex(ValueError, 'AUX_FX_NONEA does not reproduce'):
            self.run_it(s3)
        self.assertNotIn(dollar.CURRENT, s3.puts)                                                  # the warehouse's series are kept; no document goes out


if __name__ == '__main__':
    unittest.main(verbosity=2)
