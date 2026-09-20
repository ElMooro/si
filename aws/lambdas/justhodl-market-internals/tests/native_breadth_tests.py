"""Independent arithmetic and failure cases; synthetic provider fixtures are not live data."""
from pathlib import Path
from datetime import datetime
from decimal import getcontext
import copy, json, sys, unittest
sys.path.insert(0, str(Path(__file__).resolve().parents[1]/'source'))
import breadth_research_model as m

AT = '2026-09-20T12:00:00+00:00'
DAYS = m.sessions(AT)


def packet(day, rows):
    return {'status': 'OK', 'adjusted': True, 'queryCount': len(rows), 'resultsCount': len(rows), 'results': rows}


def bind(day, doc):
    raw = m.encoded(doc)
    return {'raw': raw, 'acquired_at': '2026-09-20T11:00:00+00:00', 'evidence': {
        'sha256': m.sha(raw), 'bytes': len(raw), 'provider': 'massive',
        'source_url': 'https://api.massive.com/v2/aggs/grouped/locale/us/market/stocks/'+day+'?adjusted=true&include_otc=false'}}


def fixtures():
    out = {}
    for i, day in enumerate(DAYS):
        t = int(datetime.fromisoformat(day).replace(tzinfo=m.EASTERN).timestamp()*1000)+16*3600000
        rows = [{'T': 'A', 'c': 100+i/10, 'v': 100000, 't': t},
                {'T': 'B', 'c': 300-i/10, 'v': 200000, 't': t},
                {'T': 'C', 'c': 1+i/100, 'v': 50000 if i == 252 else 5, 't': t},
                {'T': 'FILTERED_PRICE', 'c': 0.99, 'v': 100000, 't': t},
                {'T': 'FILTERED_VOLUME', 'c': 10, 'v': 49999, 't': t}]
        if i >= 60: rows.append({'T': 'IPO', 'c': 50+i/10, 'v': 100000, 't': t})
        if i != 100: rows.append({'T': 'GAPPED', 'c': 50+i/10, 'v': 100000, 't': t})
        out[day] = bind(day, packet(day, rows))
    return out


class NativeBreadthTests(unittest.TestCase):
    def setUp(self): self.inputs = fixtures()

    def compute(self): return m.compute(DAYS, self.inputs.__getitem__, AT)

    def change_latest(self, change):
        day = DAYS[-1]; doc = json.loads(self.inputs[day]['raw']); change(doc)
        self.inputs[day] = bind(day, doc)

    def test_own_denominators_and_inclusive_windows(self):
        out = self.compute(); current = out['coverage'][DAYS[-1]]
        self.assertEqual((current['current_filter_population'], current['matched_previous_session']), (5, 5))
        self.assertEqual((current['sma50_numerator'], current['sma50_denominator']), (4, 5))
        self.assertEqual((current['sma200_numerator'], current['sma200_denominator']), (2, 3))
        self.assertEqual((current['prior252_highs'], current['prior252_lows'], current['prior252_denominator']), (2, 1, 3))
        self.assertEqual(out['latest']['PCT_ABOVE_200DMA'], [DAYS[-1], 66.666667])

    def test_advance_volume_and_trin_share_one_population(self):
        out = self.compute(); latest = {k: v[1] for k, v in out['latest'].items()}
        self.assertEqual((latest['ADVANCERS'], latest['DECLINERS'], latest['UNCHANGED']), (4, 1, 0))
        self.assertEqual((latest['UP_VOLUME'], latest['DOWN_VOLUME'], latest['TRIN']), (0.35, 0.2, 2.285714))
        self.assertEqual(out['coverage'][DAYS[-1]]['up_volume_shares_decimal'], '350000')

    def test_low_historical_volume_does_not_compress_the_window(self):
        out = self.compute()
        self.assertEqual(out['coverage'][DAYS[-1]]['prior252_denominator'], 3)
        self.assertFalse(out['universe']['historical_filter_applied'])

    def test_sma_includes_today_and_uses_strict_comparison(self):
        days = DAYS[-50:]; inputs = {}
        for i, day in enumerate(days):
            t = int(datetime.fromisoformat(day).replace(tzinfo=m.EASTERN).timestamp()*1000)
            # First49 mean=100; today's50 changes the inclusive mean to99.
            inputs[day] = bind(day, packet(day, [{'T': 'X', 'c': 50 if i == 49 else 100, 'v': 100000, 't': t}]))
        out = m.compute(days, inputs.__getitem__, AT)
        self.assertEqual(out['latest']['PCT_ABOVE_50DMA'][1], 0)
        self.assertIsNone(out['latest']['PCT_ABOVE_200DMA'][1])
        inputs[days[-1]] = copy.deepcopy(inputs[days[-2]])
        # A previous day's body cannot be relabeled as the current response.
        self.assertEqual(m.compute(days, inputs.__getitem__, AT)['quality']['status'], 'unavailable')

    def test_missing_source_breaks_long_windows_and_ad_line(self):
        self.inputs[DAYS[180]] = {'error': 'HTTP_429'}
        out = self.compute()
        self.assertIsNone(out['latest']['ADVDEC_LINE'][1])
        self.assertIsNone(out['latest']['PCT_ABOVE_200DMA'][1])
        self.assertIsNone(out['latest']['NEW_HIGHS'][1])
        self.assertEqual(out['latest']['ADVANCERS'][1], 4)
        self.assertEqual(out['quality']['failed_sessions'], 1)

    def test_failed_latest_never_promotes_prior_date(self):
        self.inputs[DAYS[-1]] = {'error': 'HTTP_401'}
        out = self.compute()
        self.assertTrue(all(value == [DAYS[-1], None] for value in out['latest'].values()))
        self.assertEqual(out['quality']['status'], 'unavailable')

    def test_bad_session_counts_do_not_shrink_the_universe(self):
        self.change_latest(lambda doc: doc.update(resultsCount=len(doc['results'])-1))
        self.assertEqual(self.compute()['quality']['status'], 'unavailable')

    def test_duplicate_symbol_rejects_whole_session(self):
        def change(doc):
            doc['results'].append(doc['results'][0]); doc['queryCount'] += 1; doc['resultsCount'] += 1
        self.change_latest(change)
        self.assertEqual(self.compute()['quality']['status'], 'unavailable')

    def test_native_date_mismatch_rejects_whole_session(self):
        self.change_latest(lambda doc: doc['results'][0].update(t=doc['results'][0]['t']-86400000))
        self.assertEqual(self.compute()['quality']['status'], 'unavailable')

    def test_native_aggregate_end_is_not_invented_as_midnight(self):
        # Reviewed 5923 retained source census established intraday/end-of-window
        # timestamps; the contract binds the ET date, not an artificial midnight.
        for offset in (16*3600000, 20*3600000-1, 24*3600000-1):
            self.inputs = fixtures()
            self.change_latest(lambda doc: [row.update(t=row['t']-16*3600000+offset) for row in doc['results']])
            out = self.compute()
            self.assertEqual(out['quality']['status'], 'fresh')
            self.assertEqual(out['latest']['PCT_ABOVE_200DMA'][1], 66.666667)

    def test_source_identity_hash_and_clock_are_bound(self):
        for change in (lambda item: item['evidence'].update(sha256='0'*64),
                       lambda item: item['evidence'].update(source_url='https://example.com/data'),
                       lambda item: item.update(acquired_at='2026-09-20T13:00:00+00:00'),
                       lambda item: item.update(acquired_at='2026-09-18T11:00:00+00:00')):
            self.inputs = fixtures(); change(self.inputs[DAYS[-1]])
            self.assertEqual(self.compute()['quality']['status'], 'unavailable')

    def test_incomplete_and_unadjusted_responses_are_unavailable(self):
        for change in (lambda doc: doc.update(next_url='https://api.massive.com/next'),
                       lambda doc: doc.update(adjusted=False),
                       lambda doc: doc.update(results=[], queryCount=0, resultsCount=0),
                       lambda doc: doc['results'][0].update(c=True),
                       lambda doc: doc['results'][0].update(otc=True)):
            self.inputs = fixtures(); self.change_latest(change)
            self.assertEqual(self.compute()['quality']['status'], 'unavailable')

    def test_zero_price_changes_preserve_counts_but_not_trin(self):
        for day, item in self.inputs.items():
            doc = json.loads(item['raw'])
            for row in doc['results']:
                if row['T'] not in ('FILTERED_PRICE', 'FILTERED_VOLUME'): row['c'] = 10
            self.inputs[day] = bind(day, doc)
        out = self.compute()
        self.assertEqual(out['latest']['UNCHANGED'][1], 5)
        self.assertIsNone(out['latest']['TRIN'][1])
        self.assertEqual(out['latest']['PCT_ABOVE_50DMA'][1], 0)

    def test_replay_is_independent_of_callers_decimal_precision(self):
        expected = m.encoded(self.compute()); prior = getcontext().prec
        try:
            getcontext().prec = 5
            self.assertEqual(m.encoded(self.compute()), expected)
        finally: getcontext().prec = prior
        self.assertFalse(self.compute()['sizing_eligible'])

    def test_calendar_keeps_early_closes_and_rejects_outside_scope(self):
        self.assertEqual(m.sessions('2026-11-28T15:00:00+00:00', 2), ['2026-11-25', '2026-11-27'])
        self.assertEqual(m.sessions('2028-01-01T15:00:00+00:00', 2)[-1], '2027-12-31')
        self.assertNotIn('2026-04-03', m.sessions('2026-04-07T15:00:00+00:00', 4))
        with self.assertRaises(ValueError): m.sessions('2030-01-01T15:00:00+00:00')
        with self.assertRaises(ValueError): m.sessions('2025-10-01T15:00:00+00:00')


if __name__ == '__main__': unittest.main()
