"""The World tab's histories: reading, the day-by-day record, the choice of monthly source, compile, publication."""
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
import copy
import hashlib
import io
import json
import sys
import unittest
from pathlib import Path

HERE = Path(__file__).resolve()
sys.path[:0] = [str(HERE.parents[1] / 'source'), str(HERE.parents[3] / 'shared')]

import ciss_source_model as model          # noqa: E402
import sovereign_desk as desk              # noqa: E402
import world_history as world              # noqa: E402

NOW = datetime(2026, 11, 22, 7, 25, tzinfo=timezone.utc)
STAMP = NOW.isoformat()
TODAY = '2026-11-22'


class StorageError(Exception):
    def __init__(self, code):
        self.response = {'Error': {'Code': code}}


class MemoryS3:
    def __init__(self):
        self.objects, self.puts, self.gets = {}, [], []

    def get_object(self, Bucket, Key):
        self.gets.append(Key)
        if Key not in self.objects:
            raise StorageError('NoSuchKey')
        return {'Body': io.BytesIO(self.objects[Key]), 'LastModified': datetime(2026, 11, 21, 18, 16, tzinfo=timezone.utc)}

    def put_object(self, Bucket, Key, Body, **kw):
        self.objects[Key] = Body
        self.puts.append(Key)

    def put_json(self, key, value):
        self.objects[key] = json.dumps(value).encode()


def month_ends(first, last):
    (y, m), out = map(int, first.split('-')), []
    while '%04d-%02d' % (y, m) <= last:
        out.append(world.month_end('%04d-%02d-01' % (y, m)))
        y, m = (y + 1, 1) if m == 12 else (y, m + 1)
    return out


def firsts(first, last):
    return [d[:8] + '01' for d in month_ends(first, last)]


def days(first, last, step=1):
    day, out = date.fromisoformat(first), []
    while day <= date.fromisoformat(last):
        out.append(day.isoformat())
        day += timedelta(days=step)
    return out


def row(country, y, cds, as_of='21 November 2026', region='Europe'):
    return {'country': country, 'region': region, 'yield_10y_pct': y, 'cds_bp': cds, 'as_of': as_of, 'rating': 'A', 'cb_rate_pct': 2.0}


def publication(stamp, rows, receipt=None):
    doc = {'generated_at': stamp, 'version': '1.4.2', 'countries': rows}
    if receipt:
        doc['publication_record'] = {'manifest_key': receipt}
    return doc


def table():
    return publication('2026-11-21T18:16:35+00:00', [row('Germany', 3.303, 12.91), row('France', 4.873, 90.23), row('Turkey', 33.55, 244.51),
                                                    row('Vietnam', 4.568, None, region='Asia-Pacific'), row('Czech Republic', 5.259, None), row('Nowhere', None, None)])


def desk_documents():
    """A desk document and its history with the ECB's monthly yield for Germany (from 1990), France and Czechia."""
    ends = {'DE': month_ends('1990-01', '2026-10'), 'FR': month_ends('1986-01', '2026-10'), 'CZ': month_ends('2001-01', '2026-10')}
    doc = {'countries': [{'area': a, 'country': {'DE': 'Germany', 'FR': 'France', 'CZ': 'Czechia'}[a], 'ids': {'yield': 'Y10_' + a}} for a in ends],
           'measures': {'Y10_' + a: {'key': 'IRS.M.%s.L.L40.CI.0000.EUR.N.Z' % a} for a in ends}}
    hist = {'series': {'Y10_' + a: {'d': d, 'v': [round(2 + (i % 40) / 10, 4) for i in range(len(d))]} for a, d in ends.items()}}
    return doc, hist


def cds_documents():
    packet = {'groups': {'sovereign': {'rows': [{'name': 'Turkey', 'key': 'REPUBLIC OF TURKEY', 'iso3': 'TUR'}], 'unpriced': [{'name': 'Germany', 'key': 'FEDERAL REPUBLIC OF GERMANY'}],
                                       'dormant': [], 'universe': [{'name': 'Czech Republic', 'key': None}, {'name': 'France', 'key': 'FRENCH REPUBLIC'}]}}}
    d = days('2024-09-03', '2026-11-20', 2)
    hist = {'names': {'REPUBLIC OF TURKEY': {'points': [[x, '%.2f' % (250 + (i % 30))] for i, x in enumerate(d)], 'n': len(d)},
                      'FEDERAL REPUBLIC OF GERMANY': {'points': [['2024-09-19', 9.5]]},                 # one print is not a history
                      'FRENCH REPUBLIC': {'points': [['2025-10-06', 70.1], ['2026-11-19', 89.87], ['2026-11-19', 'n/a']]}}}
    return packet, hist


def state(**extra):
    out = {'contract': world.STATE_CONTRACT, 'reported': {}, 'oecd': {}, 'last_publication': None}
    out.update(extra)
    return out


def oecd_cache(first, last, read_at='2026-11-21T07:25:00+00:00', sid='fred:IRLTLT01DEM156N'):
    return {'id': sid, 'read_at': read_at, 'via': 'warehouse:fred-scoped/International_Data', 'sha256': 'x', 'bytes': 1, 'obs': [[d, '3.25'] for d in month_ends(first, last)]}


class Reading(unittest.TestCase):
    def test_names_are_matched_across_the_sources_spellings(self):
        self.assertEqual(world.norm('  Czech   Republic '), 'czechia')
        self.assertEqual(world.norm('Czechia'), 'czechia')
        self.assertEqual(world.norm('United States of America'), 'united states')
        self.assertEqual(world.slug('United States'), 'UNITED_STATES')
        self.assertEqual(world.slug('Czech Republic'), 'CZECHIA')
        self.assertEqual(world.slug("Côte d'Ivoire"), 'C_TE_D_IVOIRE')

    def test_the_providers_date_is_read_or_refused(self):
        self.assertEqual(world.provider_day('21 November 2026'), '2026-11-21')
        self.assertEqual(world.provider_day('3 May 2025'), '2025-05-03')
        for bad in ('31 February 2026', 'yesterday', '', None, '2026-11-21', '21 Nov 2026'):
            self.assertIsNone(world.provider_day(bad), bad)
        self.assertEqual(world.spoken('2026-09-26'), '26 September 2026')
        self.assertEqual(world.provider_day(world.spoken('2025-02-03')), '2025-02-03')

    def test_pairs_keep_one_reading_a_date_and_nothing_that_is_not_a_number(self):
        rows = [['2026-10-01', '4.5'], ['2026-09-01', 4.25], ['2026-10-01', '4.75'], ['2026-08-01', None], ['2026-07-01', '.'], ['bad', 1], ['2026-06-01', float('nan')],
                {'date': '2026-05-01', 'value': 3}, {'d': '2026-04-01', 'v': '2.5'}, 'junk', ['2026-03-01', True]]
        self.assertEqual(world.pairs(rows), [('2026-04-01', Decimal('2.5')), ('2026-05-01', Decimal('3')), ('2026-09-01', Decimal('4.25')), ('2026-10-01', Decimal('4.75'))])
        monthly = world.pairs(rows, monthly=True, today='2026-10-15')
        self.assertEqual([d for d, _ in monthly], ['2026-04-30', '2026-05-31', '2026-09-30'])       # October has not ended on the 15th
        self.assertEqual(world.pairs(None), [])

    def test_a_monthly_series_is_asked_for_again_only_when_a_new_month_can_be_there(self):
        self.assertEqual(world.expected_month_end(date(2026, 11, 22)), '2026-10-31')
        self.assertEqual(world.expected_month_end(date(2026, 12, 11)), '2026-10-31')
        self.assertEqual(world.expected_month_end(date(2026, 12, 12)), '2026-11-30')
        self.assertEqual(world.expected_month_end(date(2027, 1, 5)), '2026-11-30')
        self.assertTrue(world.due(None, NOW))
        self.assertTrue(world.due({'obs': [], 'read_at': STAMP}, NOW))
        current = oecd_cache('2020-01', '2026-10')
        self.assertFalse(world.due(current, NOW))                                               # it has October; November is not expected before 12 December
        self.assertTrue(world.due(current, datetime(2026, 12, 12, 7, 25, tzinfo=timezone.utc)))
        self.assertFalse(world.due(dict(current, read_at='2026-12-12T07:00:00+00:00'), datetime(2026, 12, 12, 9, 0, tzinfo=timezone.utc)))   # asked two hours ago
        behind = oecd_cache('2020-01', '2026-09')
        self.assertTrue(world.due(behind, NOW))
        stopped = oecd_cache('1999-01', '2018-06', read_at='2026-11-20T07:25:00+00:00')         # a series its source ended: once a week
        self.assertFalse(world.due(stopped, NOW))
        self.assertTrue(world.due(dict(stopped, read_at='2026-11-14T07:25:00+00:00'), NOW))


class Record(unittest.TestCase):
    def test_a_publication_is_folded_by_the_providers_own_day_and_the_later_one_of_a_day_wins(self):
        s = state()
        morning = publication('2026-11-20T06:17:00+00:00', [row('Turkey', 33.83, 240.0, '20 November 2026'), row('Vietnam', 4.5, None, '20 November 2026'), {'country': ''}, 'junk'])
        evening = publication('2026-11-20T18:16:00+00:00', [row('Turkey', 36.48, 241.5, '20 November 2026'), row('Vietnam', True, float('inf'), 'no date')])
        self.assertEqual(world.fold(s, morning), 3)
        self.assertEqual(world.fold(s, evening), 2)
        self.assertEqual(s['reported']['Turkey'], {'y': {'2026-11-20': 36.48}, 'c': {'2026-11-20': 241.5}})
        self.assertEqual(s['reported']['Vietnam'], {'y': {'2026-11-20': 4.5}, 'c': {}})          # a boolean and an infinity are not figures
        self.assertEqual(s['last_publication'], '2026-11-20T18:16:00+00:00')
        world.fold(s, publication('2026-11-19T18:16:00+00:00', [row('Turkey', 30.0, 1.0, 'no date')]))
        self.assertEqual(s['reported']['Turkey']['y']['2026-11-19'], 30.0)                      # without the provider's date, the publication's day
        self.assertEqual(s['last_publication'], '2026-11-20T18:16:00+00:00')                    # an older publication does not move the mark back

    def chain(self):
        """Four publications, each naming the receipt that names the one before it."""
        R = world.RETAINED
        store = {}
        stamps = ['2026-11-18T18:16:00+00:00', '2026-11-19T18:16:00+00:00', '2026-11-20T18:16:00+00:00', '2026-11-21T18:16:00+00:00']
        prior = None
        for i, stamp in enumerate(stamps):
            receipt = R + 'receipt%d.bin' % i
            store[receipt] = {'contract': 'sovereign-publication-attempt.v1', 'predecessors': {world.TABLE: {'key': prior} if prior else None}}
            doc = publication(stamp, [row('Turkey', 30 + i, 200 + i, '%d November 2026' % (18 + i))], receipt)
            prior = R + 'head%d.bin' % i
            store[prior] = doc
        return store, store[prior]

    def test_the_chain_of_publications_is_walked_back_to_the_last_one_already_folded(self):
        store, head = self.chain()
        read = store.__getitem__
        self.assertEqual([d['generated_at'][:10] for d in world.walk(read, head, None)], ['2026-11-20', '2026-11-19', '2026-11-18'])
        self.assertEqual([d['generated_at'][:10] for d in world.walk(read, head, '2026-11-19T18:16:00+00:00')], ['2026-11-20'])
        self.assertEqual(world.walk(read, head, head['generated_at']), [])
        self.assertEqual(len(world.walk(read, head, None, limit=2)), 2)
        self.assertEqual(world.walk(read, head, None, deadline=0), [])                           # no time left: nothing is read

    def test_a_broken_or_foreign_chain_simply_ends(self):
        store, head = self.chain()
        del store[world.RETAINED + 'head1.bin']                                                 # an object that cannot be read
        self.assertEqual([d['generated_at'][:10] for d in world.walk(store.__getitem__, head, None)], ['2026-11-20'])
        self.assertEqual(world.walk(store.__getitem__, publication('2026-11-21T18:16:00+00:00', []), None), [])     # no receipt named
        outside = publication('2026-11-21T18:16:00+00:00', [], 'data/elsewhere.json')
        self.assertEqual(world.walk({'data/elsewhere.json': {}}.__getitem__, outside, None), [])                     # only the engine's own kept objects are followed
        store2, head2 = self.chain()
        store2[world.RETAINED + 'receipt3.bin']['predecessors'][world.TABLE]['key'] = 'data/elsewhere.json'
        self.assertEqual(world.walk(store2.__getitem__, head2, None), [])
        store3, head3 = self.chain()
        store3[world.RETAINED + 'head2.bin']['generated_at'] = '2026-11-25T00:00:00+00:00'      # a chain that does not run backwards in time
        self.assertEqual(world.walk(store3.__getitem__, head3, None), [])


class Compile(unittest.TestCase):
    def inputs(self):
        desk_doc, desk_hist = desk_documents()
        packet, cds_hist = cds_documents()
        s = state(oecd={'Germany': oecd_cache('1956-05', '2026-10'), 'Czech Republic': oecd_cache('2000-04', '2026-10', sid='fred:IRLTLT01CZM156N'),
                        'Turkey': {'id': 'fred:x', 'read_at': STAMP, 'obs': [['2026-10-31', '1']]}})
        for i, day in enumerate(days('2026-09-27', '2026-11-21')):
            world.fold(s, publication(day + 'T18:16:00+00:00', [row('Germany', 3.3, 12.9, None), row('Turkey', 33 + i / 100, 240 + i, None), row('Vietnam', 4.5, None, None),
                                                              row('Czech Republic', 5.2, None, None), row('France', 4.8, 90, None)]))
        return table(), desk_doc, desk_hist, packet, cds_hist, s

    def test_each_country_gets_the_histories_the_site_holds_and_nothing_else(self):
        args = self.inputs()
        before = copy.deepcopy(args)
        doc = world.compile_world(*args, STAMP, inputs=[{'key': world.TABLE}], errors={})
        self.assertEqual(args, before)                                                          # pure: the inputs are left as they were
        by = {c['country']: c for c in doc['countries']}
        self.assertEqual(list(by), ['Germany', 'France', 'Turkey', 'Vietnam', 'Czech Republic', 'Nowhere'])     # the table's own order
        self.assertEqual(by['Germany']['ids'], {'yield': 'WY_GERMANY', 'yield_reported': 'RY_GERMANY', 'cds_reported': 'RC_GERMANY'})
        self.assertEqual(by['France']['ids'], {'yield': 'WY_FRANCE', 'cds': 'WC_FRANCE', 'yield_reported': 'RY_FRANCE', 'cds_reported': 'RC_FRANCE'})
        self.assertEqual(by['Turkey']['ids'], {'cds': 'WC_TURKEY', 'yield_reported': 'RY_TURKEY', 'cds_reported': 'RC_TURKEY'})
        self.assertEqual(by['Vietnam']['ids'], {'yield_reported': 'RY_VIETNAM'})
        self.assertEqual(by['Czech Republic']['ids'], {'yield': 'WY_CZECHIA', 'yield_reported': 'RY_CZECHIA'})
        self.assertEqual(by['Nowhere']['ids'], {})
        self.assertEqual(doc['missing'], {'yield': ['Turkey', 'Vietnam', 'Nowhere'], 'cds': ['Germany', 'Vietnam', 'Czech Republic', 'Nowhere'],
                                          'yield_reported': ['Nowhere'], 'cds_reported': ['Vietnam', 'Czech Republic', 'Nowhere']})
        self.assertEqual({s['id']: s['n'] for s in doc['sets']}, {'yield': 3, 'cds': 2, 'yield_reported': 5, 'cds_reported': 3})
        self.assertEqual(set(doc['measures']), set(doc['series']))
        self.assertEqual(doc['coverage'], {'countries': 6, 'measures': 13, 'by_source': {'oecd': 2, 'ecb': 1, 'dtcc': 2, 'reported': 8}, 'errors': 0, 'status': 'measured'})
        self.assertEqual(doc['reported_since'], '2026-09-27')
        self.assertEqual((doc['contract'], doc['version'], doc['calls_eligible'], doc['sizing_eligible']), ('sovereign-world.v1', world.VERSION, False, False))

    def test_the_monthly_yield_comes_from_the_source_that_reaches_furthest_back_and_is_named(self):
        doc = world.compile_world(*self.inputs(), STAMP)
        M, S = doc['measures'], doc['series']
        germany = M['WY_GERMANY']                                                               # OECD from 1956, the ECB's from 1990
        self.assertEqual((germany['source'], germany['start'], germany['date'], germany['freq'], germany['unit'], germany['status']), ('oecd', '1956-05-31', '2026-10-31', 'M', 'pct', 'current'))
        self.assertIn('IRLTLT01DEM156N', germany['source_label'])
        self.assertEqual(germany['key'], 'fred:IRLTLT01DEM156N')
        france = M['WY_FRANCE']                                                                 # only the ECB holds France here
        self.assertEqual((france['source'], france['start'], france['key']), ('ecb', '1986-01-31', 'IRS.M.FR.L.L40.CI.0000.EUR.N.Z'))
        self.assertEqual(S['WY_FRANCE']['d'][-1], '2026-10-31')
        self.assertEqual(len(S['WY_FRANCE']['d']), france['n'])
        czechia = M['WY_CZECHIA']                                                               # the table says Czech Republic, the ECB Czechia: one country
        self.assertEqual((czechia['source'], czechia['start'], czechia['country']), ('oecd', '2000-04-30', 'Czech Republic'))
        self.assertNotIn('WY_TURKEY', M)                                                        # one cached point is not a history
        for m in M.values():
            self.assertEqual(m['group'], m['set'])
            self.assertNotIn('evidence', m)

    def test_between_two_current_sources_of_equal_reach_the_ecb_is_taken_and_an_ended_one_is_kept_only_alone(self):
        e = {'source': 'ecb', 'obs': [('2001-01-31', Decimal(1)), ('2026-10-31', Decimal(2))]}
        o = {'source': 'oecd', 'obs': [('2001-01-31', Decimal(1)), ('2026-10-31', Decimal(2))]}
        self.assertEqual(world.choose_monthly([o, e], TODAY)['source'], 'ecb')
        longer = {'source': 'oecd', 'obs': [('1960-01-31', Decimal(1)), ('2026-10-31', Decimal(2))]}
        self.assertEqual(world.choose_monthly([e, longer], TODAY)['source'], 'oecd')
        ended = {'source': 'oecd', 'obs': [('1950-01-31', Decimal(1)), ('2018-06-30', Decimal(2))]}
        self.assertEqual(world.choose_monthly([e, ended], TODAY)['source'], 'ecb')              # the longer one stopped in 2018
        self.assertEqual(world.choose_monthly([ended], TODAY)['source'], 'oecd')
        self.assertIsNone(world.choose_monthly([], TODAY))

    def test_a_measured_cds_is_every_day_it_traded_and_the_tables_own_figures_every_day_kept(self):
        doc = world.compile_world(*self.inputs(), STAMP)
        M, S = doc['measures'], doc['series']
        turkey = M['WC_TURKEY']
        self.assertEqual((turkey['source'], turkey['key'], turkey['unit'], turkey['freq'], turkey['start'], turkey['date']), ('dtcc', 'REPUBLIC OF TURKEY', 'bp', 'D', '2024-09-03', '2026-11-20'))
        self.assertEqual(len(S['WC_TURKEY']['d']), len(days('2024-09-03', '2026-11-20', 2)))    # daily, not thinned to weeks
        self.assertEqual(S['WC_FRANCE'], {'d': ['2025-10-06', '2026-11-19'], 'v': [70.1, 89.87]})   # the unreadable second print of the day is left out
        self.assertNotIn('WC_GERMANY', M)                                                       # a single print
        reported = M['RY_TURKEY']
        self.assertEqual((reported['source'], reported['start'], reported['date'], reported['n'], reported['key']), ('reported', '2026-09-27', '2026-11-21', 56, world.TABLE))
        self.assertIn('kept by the site since 27 September 2026', reported['source_label'])
        self.assertEqual(S['RC_TURKEY']['v'][0], 240.0)
        self.assertEqual(M['RY_TURKEY']['status'], 'current')

    def test_the_same_inputs_and_clock_give_the_same_bytes_and_a_missing_source_costs_only_its_set(self):
        args = self.inputs()
        self.assertEqual(model.digest(world.compile_world(*args, STAMP)), model.digest(world.compile_world(*copy.deepcopy(args), STAMP)))
        t, d, h, p, c, s = args
        doc = world.compile_world(t, None, None, None, None, s, STAMP, errors={'ecb': 'UNAVAILABLE:X', 'dtcc': 'UNAVAILABLE:X'})
        self.assertEqual({x['id']: x['n'] for x in doc['sets']}, {'yield': 2, 'cds': 0, 'yield_reported': 5, 'cds_reported': 3})    # the OECD's two still stand
        self.assertEqual(doc['coverage']['status'], 'degraded')
        empty = world.compile_world(t, None, None, None, None, None, STAMP)
        self.assertEqual((empty['coverage']['measures'], len(empty['countries']), empty['reported_since']), (0, 6, None))


class Refresh(unittest.TestCase):
    def test_only_series_that_are_due_are_asked_for_one_at_a_time(self):
        s = state(oecd={'Germany': oecd_cache('1956-05', '2026-10')})
        asked, naps = [], []

        def fetch(url):
            asked.append(url)
            body = {'obs': [[d, 4.0 + i / 100] for i, d in enumerate(firsts('2025-01', '2026-11'))], 'source': 'warehouse:fred-scoped/International_Data (+2 obs tail)'}
            return body, json.dumps(body).encode()
        errors, n = world.refresh_oecd(s, ['Germany', 'Japan', 'Turkey', 'United States'], NOW, fetch, deadline=float('inf'), sleep=naps.append)
        self.assertEqual((errors, n), ({}, 2))
        self.assertEqual(asked, [world.RESOLVER + 'fred%3AIRLTLT01JPM156N', world.RESOLVER + 'fred%3AIRLTLT01USM156N'])       # Germany is current; Turkey has no OECD series
        self.assertEqual(naps, [0.5])                                                           # a pause between requests, none before the first
        japan = s['oecd']['Japan']
        self.assertEqual((japan['id'], japan['read_at'], japan['obs'][0], japan['obs'][-1][0]), ('fred:IRLTLT01JPM156N', STAMP, ['2025-01-31', '4.0'], '2026-11-30'))
        self.assertEqual(len(japan['sha256']), 64)

    def test_a_refused_series_keeps_the_one_read_before_and_says_so(self):
        behind = oecd_cache('2020-01', '2026-09')
        s = state(oecd={'Germany': copy.deepcopy(behind)})

        def fetch(url):
            raise TimeoutError('slow')
        errors, n = world.refresh_oecd(s, ['Germany', 'Japan'], NOW, fetch, deadline=float('inf'), sleep=lambda x: None)
        self.assertEqual(n, 2)
        self.assertEqual(errors, {'fred:IRLTLT01DEM156N': 'TimeoutError - the series read on 2026-11-21 is used', 'fred:IRLTLT01JPM156N': 'TimeoutError'})
        self.assertEqual(s['oecd'], {'Germany': behind})
        empty = lambda url: ({'obs': []}, b'{}')                                                 # noqa: E731
        errors, _ = world.refresh_oecd(state(), ['Japan'], NOW, empty, deadline=float('inf'), sleep=lambda x: None)
        self.assertEqual(errors, {'fred:IRLTLT01JPM156N': 'ValueError'})

    def test_out_of_time_nothing_more_is_asked(self):
        s = state()
        errors, n = world.refresh_oecd(s, ['Germany', 'Japan'], NOW, lambda url: self.fail('asked'), deadline=0, sleep=lambda x: None)
        self.assertEqual((n, list(errors)), (0, ['oecd']))


class Run(unittest.TestCase):
    def bucket(self):
        s3 = MemoryS3()
        R = world.RETAINED
        desk_doc, desk_hist = desk_documents()
        packet, cds_hist = cds_documents()
        s3.put_json(desk.CURRENT, desk_doc)
        s3.put_json(desk.HISTORY, desk_hist)
        s3.put_json(world.CDS_PACKET, packet)
        s3.put_json(world.CDS_HISTORY, cds_hist)
        prior = None
        for i, day in enumerate(('2026-11-19', '2026-11-20', '2026-11-21')):
            receipt = R + 'receipt-%s.bin' % day
            s3.put_json(receipt, {'predecessors': {world.TABLE: {'key': prior} if prior else None}})
            doc = publication(day + 'T18:16:00+00:00', [row('Germany', 3.3 + i / 10, 12.0 + i, '%d November 2026' % (19 + i)), row('Turkey', 33.0 + i, 240.0 + i, '%d November 2026' % (19 + i)),
                                                      row('Vietnam', 4.5, None, '%d November 2026' % (19 + i), 'Asia-Pacific')], receipt)
            prior = R + 'head-%s.bin' % day
            s3.put_json(prior, doc)
        s3.put_json(world.TABLE, doc)
        return s3

    def fetch(self, log):
        def inner(url):
            log.append(url)
            body = {'obs': [[d, 3.0] for d in firsts('1956-05', '2026-10')], 'source': 'warehouse:fred-scoped/International_Data'}
            return body, json.dumps(body).encode()
        return inner

    def test_a_run_folds_the_publications_it_has_not_seen_and_publishes_state_then_document(self):
        s3, asked = self.bucket(), []
        out = world.run(s3, 'b', budget_seconds=120, now=NOW, fetch=self.fetch(asked), sleep=lambda x: None)
        self.assertEqual((out['published'], out['publications_folded'], out['series_asked']), (True, 3, 1))     # Germany is the only table country with an OECD series
        self.assertEqual(out['sets'], {'yield': 1, 'cds': 1, 'yield_reported': 3, 'cds_reported': 2})
        self.assertEqual(s3.puts, [world.STATE, world.CURRENT])                                 # the record first: a failed compile must not lose a day
        doc = json.loads(s3.objects[world.CURRENT])
        self.assertEqual(doc['series']['RY_TURKEY'], {'d': ['2026-11-19', '2026-11-20', '2026-11-21'], 'v': [33.0, 34.0, 35.0]})
        self.assertEqual(doc['measures']['WY_GERMANY']['source'], 'oecd')
        self.assertEqual(doc['table'], {'key': world.TABLE, 'generated_at': '2026-11-21T18:16:00+00:00', 'version': '1.4.2', 'countries': 3})
        self.assertEqual(doc['errors'], {})
        body = dict(doc)
        sha = body.pop('sha256')
        self.assertEqual(sha, model.digest(body))
        keys = [i.get('key') or i.get('series') for i in doc['inputs']]
        self.assertEqual(keys, [world.TABLE, desk.CURRENT, desk.HISTORY, world.CDS_PACKET, world.CDS_HISTORY, 'fred:IRLTLT01DEM156N'])
        self.assertTrue(all(len(i['sha256']) == 64 for i in doc['inputs']))
        saved = json.loads(s3.objects[world.STATE])
        self.assertEqual((saved['contract'], saved['last_publication'], saved['updated_at']), (world.STATE_CONTRACT, '2026-11-21T18:16:00+00:00', STAMP))

    def test_the_next_day_only_the_new_publication_is_read_and_a_current_series_is_not_asked_for_again(self):
        s3, asked = self.bucket(), []
        world.run(s3, 'b', budget_seconds=120, now=NOW, fetch=self.fetch(asked), sleep=lambda x: None)
        R = world.RETAINED
        s3.put_json(R + 'receipt-2026-11-22.bin', {'predecessors': {world.TABLE: {'key': R + 'head-2026-11-21.bin'}}})
        s3.put_json(world.TABLE, publication('2026-11-22T18:16:00+00:00', [row('Germany', 3.6, 15.0, '22 November 2026'), row('Turkey', 36.0, 243.0, '22 November 2026')], R + 'receipt-2026-11-22.bin'))
        s3.gets.clear()
        asked.clear()
        out = world.run(s3, 'b', budget_seconds=120, now=NOW + timedelta(days=1), fetch=self.fetch(asked), sleep=lambda x: None)
        self.assertEqual((out['publications_folded'], out['series_asked'], asked), (1, 0, []))
        self.assertEqual([k for k in s3.gets if k.startswith(R)], [R + 'receipt-2026-11-22.bin', R + 'head-2026-11-21.bin'])     # one step back, then it stops
        doc = json.loads(s3.objects[world.CURRENT])
        self.assertEqual(doc['series']['RY_TURKEY']['d'][-1], '2026-11-22')
        self.assertEqual([c['country'] for c in doc['countries']], ['Germany', 'Turkey'])       # the chart lists the table's countries of the day
        self.assertNotIn('RY_VIETNAM', doc['series'])                                           # Vietnam left the table; its record is kept for the day it returns
        self.assertEqual(sorted(json.loads(s3.objects[world.STATE])['reported']['Vietnam']['y']), ['2026-11-19', '2026-11-20', '2026-11-21'])

    def test_a_source_that_cannot_be_read_is_named_and_the_other_sets_stand(self):
        s3 = self.bucket()
        del s3.objects[world.CDS_HISTORY]
        s3.objects[desk.HISTORY] = b'not json'
        out = world.run(s3, 'b', budget_seconds=120, now=NOW, fetch=lambda url: (_ for _ in ()).throw(OSError('down')), sleep=lambda x: None)
        doc = json.loads(s3.objects[world.CURRENT])
        self.assertEqual(doc['errors'], {'ecb': 'UNAVAILABLE:JSONDecodeError', 'dtcc': 'UNAVAILABLE:StorageError', 'fred:IRLTLT01DEM156N': 'OSError'})
        self.assertEqual(out['sets'], {'yield': 0, 'cds': 0, 'yield_reported': 3, 'cds_reported': 2})
        self.assertEqual(doc['coverage']['status'], 'degraded')

    def test_without_the_table_nothing_is_published_and_an_unreadable_record_starts_again(self):
        s3 = self.bucket()
        s3.objects[world.STATE] = b'{"contract": "something else"}'
        out = world.run(s3, 'b', budget_seconds=120, now=NOW, fetch=self.fetch([]), sleep=lambda x: None)
        self.assertEqual(out['publications_folded'], 3)
        self.assertIn('state', json.loads(s3.objects[world.CURRENT])['errors'])
        empty = MemoryS3()
        with self.assertRaises(StorageError):
            world.run(empty, 'b', now=NOW, fetch=self.fetch([]))
        self.assertEqual(empty.puts, [])
        empty.put_json(world.TABLE, {'countries': 'none'})
        with self.assertRaises(ValueError):
            world.run(empty, 'b', now=NOW, fetch=self.fetch([]))
        self.assertEqual(empty.puts, [])


if __name__ == '__main__':
    unittest.main(verbosity=2)
