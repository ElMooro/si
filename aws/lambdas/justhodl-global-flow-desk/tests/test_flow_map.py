"""The money map (flow_map.py) on real warehouse rows: 135 funds, 75 days and their provider profiles, cut by ops 6962."""
from datetime import datetime, timezone
from pathlib import Path
import gzip
import io
import json
import math
import unittest

import flow_map as M

HERE = Path(__file__).resolve().parent
CUT = json.load(gzip.open(HERE / 'fixtures' / 'flow-map-warehouse-cut.json.gz'))
NOW = datetime(2026, 11, 26, 4, 0, tzinfo=timezone.utc)


def real():
    series = {t: M.merge_rows([M.compact(x) for x in f['rows']]) for t, f in CUT['funds'].items()}
    profiles = {t: f['profile'] for t, f in CUT['funds'].items() if f.get('profile')}
    return series, profiles


SERIES, PROFILES = real()
DOC, FDOC, HIST, SHARDS = M.build(SERIES, PROFILES, {}, NOW, ['2026-10', '2026-11'])
COL = FDOC['cols'].index
ROW = {r[0]: r for r in FDOC['rows']}


def cell(t, name):
    return ROW[t][COL(name)]


class RealCut(unittest.TestCase):
    def test_what_was_built(self):
        self.assertEqual((DOC['contract'], DOC['version'], DOC['as_of']), ('global-flow-map.v1', '3.0.0', '2026-11-24'))
        self.assertEqual([w['key'] for w in DOC['windows']], ['d1', 'w1', 'm1', 'm3', 'ytd', 'y1'])
        self.assertEqual([(w['n'], w['complete']) for w in DOC['windows'][:4]], [(1, True), (5, True), (21, True), (63, True)])
        self.assertFalse(DOC['windows'][5]['complete'])                 # 75 days of history are not a year, and the document says so
        q = DOC['quality']
        self.assertEqual(q['funds_with_rows'], 135)
        self.assertEqual(q['funds_current'] + q['funds_late'], 135)
        self.assertEqual(q['funds_unclassified'], 0)
        for row in FDOC['rows']:
            self.assertEqual(len(row), len(FDOC['cols']))
        json.loads(M.encode(DOC)), json.loads(M.encode(FDOC)), json.loads(M.encode(HIST))        # strict JSON: no NaN, no Infinity

    def test_a_window_is_the_share_counts_path_and_equals_the_providers_days(self):
        for t in ('VOO', 'SPY', 'XLE', 'SGOV'):
            rows = SERIES[t]
            path = [(b[1] - a[1]) * a[2] for a, b in zip(rows, rows[1:])]       # (shares - shares the row before) x the NAV of the row before
            self.assertAlmostEqual(cell(t, 'w1'), sum(path[-5:]), places=1)
            self.assertAlmostEqual(cell(t, 'd1'), path[-1], places=1)
            self.assertAlmostEqual(cell(t, 'm1'), sum(path[-21:]), places=1)
            base = rows[-6][1] * rows[-6][2]                                # shares x NAV on the day before the five days began
            self.assertAlmostEqual(cell(t, 'p_w1'), 100 * sum(path[-5:]) / base, places=2)
            # ... and on rows that follow each other day by day that is the provider's own figure
            self.assertAlmostEqual(sum(path[-5:]), sum(r[3] for r in rows[-5:]), delta=5.0)
        self.assertAlmostEqual(cell('VOO', 'w1'), 12828781140.46, delta=5.0)
        self.assertAlmostEqual(cell('SPY', 'w1'), -6060084785.8, delta=5.0)
        pf = DOC['quality']['provider_figure']
        self.assertGreater(pf['rows_equal'], 0.99 * pf['rows_compared'])
        self.assertEqual(pf['rows_compared'], pf['rows_equal'] + pf['rows_differing'])

    def test_share_splits_are_taken_out(self):
        splits = {(t, d): x for t, d, x in DOC['quality']['splits_taken_out']}
        self.assertEqual(splits[('SOXX', '2026-11-04')], 'split 3-for-1')
        self.assertEqual(splits[('QUAL', '2026-11-04')], 'split 4-for-1')
        self.assertEqual(splits[('MTUM', '2026-11-04')], 'split 4-for-1')
        self.assertEqual(splits[('ROM', '2026-11-19')], 'split 3-for-1')
        self.assertEqual(splits[('QID', '2026-11-19')], 'split 1-for-4')
        self.assertEqual(splits[('ETHA', '2026-10-05')], 'split 1-for-3')
        # the provider printed +$89.5 billion for SOXX and +$144 billion for QUAL on the day of their splits
        raw = {r[0]: r for r in SERIES['SOXX']}
        self.assertGreater(raw['2026-11-04'][3], 8.9e10)
        clean = {r[0]: r for r in M.clean_series(SERIES['SOXX'])}
        self.assertAlmostEqual(clean['2026-11-04'][3], (286800000.0 - 3 * 94400000.0) * SERIES['SOXX'][[r[0] for r in SERIES['SOXX']].index('2026-11-03')][2] / 3, delta=1.0)
        self.assertAlmostEqual({r[0]: r for r in M.clean_series(SERIES['QID'])}['2026-11-19'][3], 125000.0 * 16.2573 * 4, delta=1.0)
        self.assertLess(abs(cell('QUAL', 'm1')), 2e9)                       # not 144 billion
        self.assertEqual(cell('ROM', 'w1'), 0.0)                            # not +1.885 billion
        self.assertIn('2026-11-19 split 3-for-1', cell('ROM', 'flags'))
        # a 30% one-day fall in a three-times fund with no change in shares is a market move, not a split
        self.assertFalse(any(t == 'SOXL' for t, _, _ in DOC['quality']['splits_taken_out']))

    def test_every_fund_is_counted_once(self):
        cur = [r for r in FDOC['rows'] if r[COL('behind')] <= M.LATE_DAYS]
        self.assertEqual(len(cur), DOC['quality']['funds_current'])
        self.assertEqual(sum(g['n'] for g in DOC['groups']['segments']), len(cur))
        for key in ('d1', 'w1', 'm1', 'm3', 'ytd'):
            want = sum(r[COL(key)] or 0 for r in cur)
            self.assertAlmostEqual(sum(g['w'][key]['net'] for g in DOC['groups']['segments']), want, delta=5.0)
            self.assertAlmostEqual(DOC['totals']['all']['w'][key]['net'], want, delta=5.0)
            plain = sum(r[COL(key)] or 0 for r in cur if r[COL('seg')] != 'Leveraged & inverse')
            self.assertAlmostEqual(sum(g['w'][key]['net'] for g in DOC['groups']['classes']), plain, delta=5.0)
            self.assertAlmostEqual(DOC['totals']['plain']['w'][key]['net'] + DOC['totals']['leveraged']['w'][key]['net'], want, delta=5.0)
        for g in DOC['groups']['segments'] + DOC['groups']['sectors'] + DOC['groups']['classes']:
            for w in g['w'].values():
                self.assertAlmostEqual(w['in'] + w['out'], w['net'], delta=0.05)
                self.assertTrue(w['in'] >= 0 >= w['out'] and w['n_in'] + w['n_out'] <= g['n'])

    def test_a_late_fund_is_left_out_and_named(self):
        self.assertGreater(cell('IVV', 'behind'), M.LATE_DAYS)             # its per-fund file in the cut ends 2026-11-10
        late = {t for t, _, _ in DOC['quality']['late_largest']}
        self.assertIn('IVV', late)
        us = next(g for g in DOC['groups']['segments'] if g['id'] == 'Equity · United States')
        self.assertNotIn('IVV', [t for t, _ in us['w']['m1']['top_in'] + us['w']['m1']['top_out']])
        self.assertNotIn('IVV', [x[0] for b in DOC['boards'].values() for side in b.values() for x in side])

    def test_classes_come_from_the_providers_own_fields(self):
        want = {'SPY': 'Equity · United States', 'VOO': 'Equity · United States', 'QQQ': 'Equity · United States', 'XLK': 'Equity · sector funds',
                'TLT': 'Bonds · Treasuries & agencies', 'BIL': 'Bonds · Treasuries & agencies', 'TIP': 'Bonds · TIPS', 'HYG': 'Bonds · high yield', 'LQD': 'Bonds · corporate',
                'MUB': 'Bonds · municipal', 'EMB': 'Bonds · international & emerging', 'BNDX': 'Bonds · international & emerging', 'AGG': 'Bonds · broad & other',
                'GLD': 'Commodities · precious metals', 'USO': 'Commodities · energy', 'DBC': 'Commodities · broad & other', 'IBIT': 'Crypto', 'ETHA': 'Crypto',
                'UUP': 'Currencies', 'VXX': 'Volatility', 'VNQ': 'Real estate', 'SQQQ': 'Leveraged & inverse', 'TQQQ': 'Leveraged & inverse', 'SOXL': 'Leveraged & inverse',
                'EEM': 'Equity · emerging markets', 'INDA': 'Equity · emerging markets', 'EWJ': 'Equity · international developed', 'VT': 'Equity · global',
                'VXUS': 'Equity · international developed'}
        for t, seg in want.items():
            self.assertEqual(M.classify(PROFILES[t])['seg'], seg, t)
        self.assertEqual(M.classify(PROFILES['XLK'])['sec'], 'Technology')
        self.assertEqual(M.classify(PROFILES['XLE'])['sec'], 'Energy')
        self.assertEqual(M.classify(PROFILES['VNQ'])['sec'], 'Real estate')
        self.assertEqual((M.classify(PROFILES['IBIT'])['sub'], M.classify(PROFILES['ETHA'])['sub']), ('Bitcoin', 'Ether'))
        self.assertEqual((M.classify(PROFILES['EWJ'])['cty'], M.classify(PROFILES['INDA'])['cty'], M.classify(PROFILES['SPY'])['cty']), ('Japan', 'India', None))
        self.assertEqual((M.classify(PROFILES['BIL'])['mat'], M.classify(PROFILES['TLT'])['mat']), ('Short (under 3 years)', 'Long (10 years and over)'))
        self.assertEqual(M.classify(PROFILES['SQQQ'])['sub'], 'Inverse · Equity')
        self.assertEqual(M.classify(PROFILES['TQQQ'])['sub'], 'Leveraged long · Equity')
        self.assertEqual(M.classify(None)['seg'], 'Unclassified')
        # a leveraged fund is in no sector, country or class table: a bet against the market is never netted against a purchase
        for kind in ('sectors', 'classes', 'countries', 'categories'):
            tops = {t for g in DOC['groups'][kind] for w in g['w'].values() for t, _ in w['top_in'] + w['top_out']}
            self.assertFalse(tops & {'SQQQ', 'TQQQ', 'SOXL', 'ROM', 'QID'}, kind)

    def test_sectors_two_ways(self):
        direct = {g['id']: g for g in DOC['groups']['sectors']}
        self.assertTrue({'Technology', 'Energy', 'Financials', 'Health care'} <= set(direct))
        w = M.sector_weights(PROFILES['XLK'])
        self.assertGreater(w['Technology'], 0.95)
        self.assertLessEqual(sum(w.values()), 1.0 + 1e-9)
        self.assertNotIn('cash_or_derivatives', w)
        self.assertEqual(M.sector_weights(PROFILES['GLD']), {})
        # through all equity funds: each fund's flow times the provider's weight of the sector
        cur = {r[0]: r for r in FDOC['rows'] if r[COL('behind')] <= M.LATE_DAYS and r[COL('seg')] != 'Leveraged & inverse' and r[COL('ac')] in ('Equity', 'Real Estate')}
        tech = sum((r[COL('w1')] or 0) * M.sector_weights(PROFILES[t]).get('Technology', 0) for t, r in cur.items())
        got = next(g for g in DOC['groups']['sectors_through'] if g['id'] == 'Technology')
        self.assertAlmostEqual(got['w']['w1']['net'], tech, delta=5.0)
        self.assertGreater(abs(got['w']['w1']['net']), abs(direct['Technology']['w']['w1']['net']))    # VOO's and QQQ's money counts here

    def test_boards_and_turns(self):
        b = DOC['boards']['w1']
        self.assertEqual(b['in_usd'][0][0], 'VOO')
        self.assertEqual(b['out_usd'][0][0], 'SPY')
        self.assertTrue(all(x[1] > 0 for x in b['in_usd']) and all(x[1] < 0 for x in b['out_usd']))
        self.assertEqual([x[1] for x in b['in_usd']], sorted((x[1] for x in b['in_usd']), reverse=True))
        self.assertTrue(all(x[2] > 0 for x in b['in_pct']) and all(x[2] < 0 for x in b['out_pct']))
        for t, now, before in DOC['turned']['to_in']:
            self.assertTrue(now > 0 > before, t)
        for t, now, before in DOC['turned']['to_out']:
            self.assertTrue(now < 0 < before, t)

    def test_history_and_fund_shards(self):
        self.assertEqual(HIST['dates'][-1], '2026-11-24')
        seg = HIST['series']['segments:Equity · United States']
        self.assertEqual(len(seg), len(HIST['dates']))
        cur = [t for t, r in ROW.items() if r[COL('behind')] <= M.LATE_DAYS and r[COL('seg')] == 'Equity · United States']
        on_day = {t: {r[0]: r[3] for r in SERIES[t]} for t in cur}
        untouched = [t for t in cur if not cell(t, 'flags')]
        self.assertGreater(len(untouched), 0.8 * len(cur))
        mine = {t: {r[0]: r[3] for r in M.clean_series(SERIES[t])} for t in cur}
        self.assertAlmostEqual(seg[-1], sum(v.get('2026-11-24') or 0 for v in mine.values()), delta=len(cur))
        self.assertAlmostEqual(seg[-2], sum(v.get('2026-11-23') or 0 for v in mine.values()), delta=len(cur))
        for t in untouched[:40]:                                               # the provider's figure, where nothing was done
            if SERIES[t][-1][0] == '2026-11-24' and SERIES[t][-2][0] == '2026-11-23':
                self.assertAlmostEqual(mine[t]['2026-11-24'], on_day[t]['2026-11-24'], delta=max(2000.0, 0.003 * abs(on_day[t]['2026-11-24'])), msg=t)
        self.assertAlmostEqual(sum(seg[-5:]), next(g for g in DOC['groups']['segments'] if g['id'] == 'Equity · United States')['w']['w1']['net'], delta=5 * len(cur))
        spy = SHARDS['S']['funds']['SPY']
        self.assertEqual(len(spy['f']), len(SHARDS['S']['dates']))
        self.assertAlmostEqual(spy['f'][-1], SERIES['SPY'][-1][3], delta=5)
        self.assertAlmostEqual(spy['a'][-1], SERIES['SPY'][-1][1] * SERIES['SPY'][-1][2], delta=1)
        self.assertEqual(SHARDS['S']['funds']['SOXX']['x'], {'2026-11-04': 'split 3-for-1'})


class Rules(unittest.TestCase):
    def test_later_processed_row_wins_and_days_sort(self):
        rows = M.merge_rows([('2026-11-20', '2026-11-20', 10.0, 5.0, 1.0), ('2026-11-19', '2026-11-19', 9.0, 5.0, 0.0)], [('2026-11-20', '2026-11-21', 11.0, 5.0, 2.0)])
        self.assertEqual(rows, [('2026-11-19', 9.0, 5.0, 0.0), ('2026-11-20', 11.0, 5.0, 2.0)])
        self.assertIsNone(M.compact({'effective_date': None, 'fund_flow': 1}))
        self.assertEqual(M.compact({'effective_date': '2026-11-20', 'processed_date': '2026-11-20', 'shares_outstanding': '3', 'nav': 2, 'fund_flow': None}),
                         ('2026-11-20', '2026-11-20', 3.0, 2.0, None))

    def test_a_count_that_comes_straight_back_is_a_round_trip(self):
        rows = [('d1', 100.0, 10.0, 0.0), ('d2', 1000.0, 10.0, 9000.0), ('d3', 100.0, 10.1, -9000.0), ('d4', 110.0, 10.0, 101.0)]
        clean, notes = M.count_path(rows)
        self.assertEqual([r[3] for r in clean], [0.0, 0.0, 0.0, 101.0])
        self.assertEqual([r[4] for r in clean], [None, 'round trip', 'round trip', None])
        self.assertEqual([(x['k'], x['usd']) for x in notes], [('trip', 9000.0)])
        self.assertEqual([r[1] for r in clean], [100.0, 100.0, 100.0, 110.0])       # the count the fund really had
        # a creation that stays is money, however large, and is flagged as a jump
        stay, notes = M.count_path([('d1', 100.0, 10.0, 0.0), ('d2', 1000.0, 10.0, 9000.0), ('d3', 1000.0, 10.1, 0.0), ('d4', 1000.0, 10.1, 0.0), ('d5', 990.0, 10.1, 0.0)])
        self.assertEqual([r[3] for r in stay], [0.0, 9000.0, 0.0, 0.0, -101.0])
        self.assertEqual(stay[1][4], 'jump')
        # real rows (ops 6965): the Power Buffer December fund doubled for two rows and came back
        pdec = [('2026-11-13', 20.65e6, 47.21, 0.0), ('2026-11-16', 20.65e6, 46.43, 0.0), ('2026-11-17', 41.3e6, 45.99, 958.8e6), ('2026-11-18', 41.3e6, 45.86, 0.0),
                ('2026-11-19', 20.65e6, 45.05, -947.0e6), ('2026-11-20', 20.65e6, 43.92, 0.0)]
        clean, notes = M.count_path(pdec)
        self.assertEqual([round(r[3]) for r in clean], [0, 0, 0, 0, 0, 0])
        self.assertEqual([r[4] for r in clean][2:5], ['round trip'] * 3)
        self.assertAlmostEqual(notes[0]['usd'], 20.65e6 * 46.43, delta=1)

    def test_a_week_long_hump_is_a_round_trip_too(self):
        # real rows (ops 6964): ARK Autonomous Technology. The provider's counts for ARK and Alger funds swelled from 11 to 19 November
        # 2026 and came back; its figures say +$2.45 billion in and -$2.22 billion out within three days
        arkq = [('2026-11-13', 15.9e6, 107.99, 0.0), ('2026-11-16', 15.9e6, 103.44, 0.0), ('2026-11-17', 15.9e6, 102.32, 0.0), ('2026-11-18', 22.75e6, 101.58, 700.9e6),
                ('2026-11-19', 40e6, 98.36, 1.75e9), ('2026-11-20', 17.4e6, 90.98, -2.22e9), ('2026-11-23', 16.9e6, 98.51, -45.5e6), ('2026-11-24', 15.35e6, 95.76, -152.7e6)]
        clean, notes = M.count_path(arkq)
        self.assertEqual([r[4] for r in clean], [None, None, None, 'round trip', 'round trip', 'round trip', None, None])
        self.assertEqual([round(r[3] / 1e6, 1) for r in clean], [0.0, 0.0, 0.0, 0.0, 0.0, 147.5, -45.5, -152.7])
        # what the trip left behind, at the NAV of the row before it came back; then the ordinary days
        self.assertAlmostEqual(sum(r[3] for r in clean), (17.4e6 - 15.9e6) * 98.36 + (16.9e6 - 17.4e6) * 90.98 + (15.35e6 - 16.9e6) * 98.51, delta=1)
        self.assertEqual([r[1] for r in clean][3:5], [15.9e6, 15.9e6])                  # the count the fund had, for its assets on those days
        # ... and a jump of which less than two thirds comes back is money both ways
        half = [('d%d' % i, v, 10.0, 0.0) for i, v in enumerate([100.0, 100.0, 200.0, 200.0, 160.0, 160.0, 160.0])]
        self.assertEqual([r[3] for r in M.clean_series(half)], [0.0, 0.0, 1000.0, 0.0, -400.0, 0.0, 0.0])
        # eleven rows later is too late to be a round trip
        late = [('d%02d' % i, v, 10.0, 0.0) for i, v in enumerate([100.0, 200.0] + [200.0] * 10 + [100.0])]
        self.assertEqual((M.clean_series(late)[1][3], M.clean_series(late)[-1][3]), (1000.0, -1000.0))

    def test_the_flow_is_the_count_path_not_the_providers_figure(self):
        clean = M.clean_series([('d1', 100.0, 10.0, 0.0), ('d2', 120.0, 11.0, None)])
        self.assertEqual(clean[1][3], 200.0)
        # real rows (ops 6965): FT Vest Deep Buffer November. Two days are missing and the provider's figure for the next row says
        # -$346.9 million while the share count has not moved
        dnov = [('2026-11-16', 7.775e6, 52.81, 0.0), ('2026-11-17', 7.75e6, 52.46, -1.3e6), ('2026-11-18', 7.75e6, 52.44, 0.0), ('2026-11-23', 7.75e6, 50.54, -346.9e6),
                ('2026-11-24', 7.9e6, 50.43, 7.6e6)]
        clean = M.clean_series(dnov)
        self.assertEqual(clean[3][3], 0.0)
        self.assertAlmostEqual(sum(r[3] for r in clean[1:]), (7.9e6 - 7.775e6) * 52.0, delta=0.3e6)
        # real rows: Alger Concentrated Equity. 2026-11-17 and 11-18 are missing; the provider's figures for the five days sum to
        # -$9.31 billion, more than the fund held. The count path: the fund went from 232.9M shares to 25.77M
        cneq = [('2026-11-16', 232.9e6, 34.77, 292.0e6), ('2026-11-19', 240.7e6, 33.89, -2.17e9), ('2026-11-20', 95.05e6, 31.54, -4.93e9), ('2026-11-23', 32.66e6, 34.28, -1.97e9),
                ('2026-11-24', 25.77e6, 33.13, -236.1e6)]
        clean, notes = M.count_path(cneq)
        self.assertAlmostEqual(sum(r[3] for r in clean[1:]), 7.8e6 * 34.77 - 145.65e6 * 33.89 - 62.39e6 * 31.54 - 6.89e6 * 34.28, delta=1e6)
        self.assertLess(abs(sum(r[3] for r in clean[1:])), 232.9e6 * 34.77)          # a fund cannot lose more than it had
        self.assertEqual([r[4] for r in clean], [None, None, 'jump', 'jump', None])  # jumps that stay are money and are named
        self.assertEqual([x['k'] for x in notes], ['jump', 'jump'])

    def test_a_row_of_another_fund_is_dropped(self):
        # real rows (ops 6965): Fidelity Solana Fund. One row carries another fund's count and NAV; the provider prints +$2.02B then -$12.72B
        fsol = [('2026-11-02', 17.15e6, 10.82, 0.0), ('2026-11-03', 17.15e6, 11.06, 0.0), ('2026-11-04', 199.9e6, 69.62, 2.02e9), ('2026-11-05', 17.15e6, 10.64, -12.72e9),
                ('2026-11-06', 17.15e6, 10.63, 0.0), ('2026-11-10', 17.07e6, 9.86, -769071.0)]
        clean, notes = M.count_path(fsol)
        self.assertEqual([r[0] for r in clean], ['2026-11-02', '2026-11-03', '2026-11-05', '2026-11-06', '2026-11-10'])
        self.assertEqual([round(r[3]) for r in clean[:4]], [0, 0, 0, 0])
        self.assertAlmostEqual(clean[4][3], -0.08e6 * 10.63, delta=1)
        self.assertEqual(notes[0]['k'], 'misprint')

    def test_recorded_splits(self):
        # real rows (ops 6965): Vanguard Information Technology, 8-for-1 on 2026-04-21. The provider restated the NAV on the 20th and the
        # count on the 21st, and printed +$106.26 billion of inflow
        vgt = [('2026-04-16', 150.4e6, 792.03, 60.7e6), ('2026-04-17', 150.5e6, 805.58, 62.9e6), ('2026-04-20', 150.7e6, 100.70, 161.1e6), ('2026-04-21', 1206e6, 100.97, 106.26e9),
               ('2026-04-22', 1207e6, 103.04, 128.7e6), ('2026-04-23', 1208e6, 101.56, 81.1e6)]
        clean, notes = M.count_path(vgt, [('2026-04-21', 8.0)])
        self.assertEqual(clean[3][4], 'split 8-for-1')
        self.assertAlmostEqual(clean[3][3], (1206e6 - 8 * 150.7e6) * 100.70, delta=1)
        self.assertLess(max(abs(r[3]) for r in clean), 3e8)
        self.assertEqual([round(r[1] / 1e6) for r in clean][:3], [1203, 1204, 1206])       # earlier counts on today's basis
        self.assertAlmostEqual(clean[0][2], 792.03 / 8, places=6)                          # and the earlier NAVs, but not the one already restated
        self.assertEqual(clean[2][2], 100.70)
        self.assertIn('on record', notes[0]['x'])
        # without the record nothing proves a split when the NAV does not move on the same or the next row ... here it does (the row before)
        self.assertEqual(M.clean_series(vgt)[3][4], 'split 8-for-1')
        # real rows: T-Rex 2X Long MSTR, 1-for-10 recorded 2026-08-24: the count moved on the 21st and the NAV on the 24th
        mstu = [('2026-08-19', 188.6e6, 2.11, 4.0e6), ('2026-08-20', 177.9e6, 2.44, -22.6e6), ('2026-08-21', 17.71e6, 2.73, -390.2e6), ('2026-08-24', 17.6e6, 28.82, -300146.0),
                ('2026-08-25', 18.45e6, 30.48, 24.5e6)]
        clean, _ = M.count_path(mstu, [('2025-12-03', 0.1), ('2026-08-24', 0.1)])
        self.assertEqual(clean[2][4], 'split 1-for-10')
        self.assertAlmostEqual(clean[2][3], (17.71e6 - 17.79e6) * 24.4, delta=1e4)         # -$2 million, not -$390 million
        self.assertAlmostEqual(clean[3][3], (17.6e6 - 17.71e6) * 27.3, delta=1e4)
        # real rows: Leverage Shares 2X Long BAIG, 1-for-10 recorded 2026-05-05, with a real creation on the same day
        baig = [('2026-05-01', 4.24e6, 3.73, -311400.0), ('2026-05-04', 4.58e6, 3.78, 1.3e6), ('2026-05-05', 0.578e6, 37.27, -15.1e6), ('2026-05-06', 0.578e6, 41.33, 0.0)]
        clean, _ = M.count_path(baig, [('2026-05-05', 0.1)])
        self.assertEqual(clean[2][4], 'split 1-for-10')
        self.assertAlmostEqual(clean[2][3], (0.578e6 - 0.458e6) * 37.8, delta=1e3)         # +$4.5 million in, not -$15.1 million out
        # a recorded split the counts never show is said, and nothing is changed
        flat = [('2026-02-05', 15e6, 6.90, 0.0), ('2026-02-06', 15e6, 6.55, 0.0), ('2026-02-09', 15e6, 62.29, 0.0), ('2026-02-10', 15e6, 63.49, 0.0)]
        clean, notes = M.count_path(flat, [('2026-02-09', 0.1)])
        self.assertEqual(([r[3] for r in clean], [r[4] for r in clean], notes[0]['k']), ([0.0] * 4, [None] * 4, 'unseen'))
        # a leveraged fund falling a third while buyers add half again is not a 3-for-2 split
        dip = [('2026-07-01', 10e6, 30.0, 0.0), ('2026-07-02', 15e6, 20.0, 150e6), ('2026-07-06', 15e6, 21.0, 0.0), ('2026-07-07', 15e6, 21.0, 0.0), ('2026-07-08', 15e6, 21.0, 0.0)]
        self.assertEqual((M.clean_series(dip)[1][3], M.clean_series(dip)[1][4]), (150e6, 'jump'))

    def test_split_ratio_names(self):
        self.assertEqual((M.ratio_name(3.0), M.ratio_name(0.25), M.ratio_name(1.5), M.ratio_name(8.0027 / 1.0003), M.ratio_name(0.1)), ('3-for-1', '1-for-4', '3-for-2', '8-for-1', '1-for-10'))
        self.assertEqual((M.ratio_name(11.0), M.ratio_name(1 / 241.0)), ('11-for-1', '1-for-241'))
        self.assertTrue(all(c > 0 and abs(math.log(c)) > 0.2 for c in M.RATIOS))

    def test_china_is_counted_with_its_offshore_registrations(self):
        base = {'asset_class': 'Equity', 'category': 'Size and Style', 'focus': 'Large Cap', 'region': 'Asia-Pacific', 'development_class': 'Emerging Markets'}
        geo = {'FXI': {'cn': 0.538, 'ky': 0.371, 'hk': 0.052}, 'KWEB': {'ky': 0.484, 'us': 0.261, 'hk': 0.105, 'bm': 0.014, 'cn': 0.007}, 'KTEC': {'ky': 0.639, 'hk': 0.165, 'cn': 0.098},
               'EWH': {'hk': 0.765, 'ky': 0.16, 'bm': 0.058}, 'EWT': {'tw': 0.965, 'ky': 0.029}, 'CQQQ': {'ky': 0.312, 'cn': 0.205, 'hk': 0.184, 'us': 0.154}}       # the provider's weights, ops 6964
        got = {t: M.classify(dict(base, geographic_exposure=g))['cty'] for t, g in geo.items()}
        self.assertEqual(got, {'FXI': 'China', 'KWEB': 'China', 'KTEC': 'China', 'EWH': 'Hong Kong', 'EWT': 'Taiwan', 'CQQQ': 'China'})
        # the same offshore weights in a fund the provider places in North America are not China
        self.assertIsNone(M.classify(dict(base, region='North America', geographic_exposure={'ky': 0.7, 'us': 0.3}))['cty'])
        # a "sector" fund whose focus is not a sector is in no sector row; natural resources keeps the provider's name
        self.assertIsNone(M.classify({'asset_class': 'Equity', 'category': 'Sector', 'focus': 'Strategy'})['sec'])
        self.assertEqual(M.classify({'asset_class': 'Equity', 'category': 'Sector', 'focus': 'Natural Resources'})['sec'], 'Natural resources')

    def test_reporting_days_drop_a_thin_day(self):
        series = {'F%d' % i: [('2026-11-%02d' % d, 1.0, 1.0, 0.0) for d in (16, 17, 18, 19, 20, 23)] for i in range(40)}
        series['F0'].append(('2026-11-21', 1.0, 1.0, 0.0))                 # one fund printed on a Saturday
        self.assertEqual(M.calendar(series), ['2026-11-%02d' % d for d in (16, 17, 18, 19, 20, 23)])

    def test_issuer_check(self):
        days = ['2026-11-%02d' % d for d in (2, 3, 4, 5, 6, 9, 10, 11, 12, 13, 16, 17, 18, 19, 20, 23, 24)]
        counts = [100.0 + 3 * i for i in range(len(days))]
        series = {'AAA': [(d, c, 10.0, 30.0) for d, c in zip(days, counts)], 'BBB': [(d, c, 10.0, 30.0) for d, c in zip(days, counts)],
                  'CCC': [(d, c, 10.0, 30.0) for d, c in zip(days, counts)]}
        series.update({'P%d' % i: [(d, 1.0, 1.0, 0.0) for d in days] for i in range(5)})
        issuer = {'AAA': {b: counts[i] for i, b in enumerate(days[1:])},            # the issuer's file shows each count a day later
                  'BBB': dict(zip(days, counts)),                                   # the issuer's file is on the day itself
                  'CCC': {d: c + 7.0 * (i % 3) + 1 for i, (d, c) in enumerate(zip(days, counts))}}
        doc, fdoc, _, _ = M.build(series, {}, issuer, NOW, ['2026-11'])
        chk = {r[0]: r[fdoc['cols'].index('chk')] for r in fdoc['rows']}
        self.assertEqual((chk['AAA'], chk['BBB'], chk['CCC'], chk['P0']), (1, 0, -1, None))
        c = doc['quality']['issuer_check']
        self.assertEqual((c['funds'], c['next_day'], c['same_day'], c['no_match']), (3, 1, 1, ['CCC']))


class FakeS3:
    def __init__(self, store):
        self.store, self.puts = dict(store), []

    def get_object(self, Bucket, Key):
        if Key not in self.store:
            raise KeyError(Key)
        return {'Body': io.BytesIO(self.store[Key])}

    def put_object(self, Bucket, Key, Body, **kw):
        self.puts.append((Key, kw))
        self.store[Key] = Body

    def get_paginator(self, name):
        store = self.store

        class P:
            def paginate(self, Bucket, Prefix):
                yield {'Contents': [{'Key': k} for k in sorted(store) if k.startswith(Prefix)]}
        return P()


def warehouse(n_copies=1):
    """The cut laid out as the bank lays it out: long per-fund files that stop early, by-date months that bring them up to date."""
    store = {}
    months = {}
    for t, f in CUT['funds'].items():
        old = [x for x in f['rows'] if x['effective_date'] < '2026-11-10']
        store[M.PER_FUND + t + '.json.gz'] = gzip.compress(json.dumps(old).encode())
        for x in f['rows']:
            if x['effective_date'] >= '2026-10-01':
                months.setdefault(x['effective_date'][:7], []).append(x)
        if f.get('profile'):
            store[M.PROFILES + t + '.json.gz'] = gzip.compress(json.dumps({'results': [f['profile']]}).encode())
    for m, rows in months.items():
        store[M.BY_DATE + m + '.json.gz'] = gzip.compress(json.dumps(rows).encode())
    return store


class Run(unittest.TestCase):
    def test_reads_the_warehouse_and_refuses_a_thin_one(self):
        s3 = FakeS3(warehouse())
        summary, (doc, fdoc, _, _) = M.run(s3, 'b', NOW, dry_run=False, log=lambda *_: None)
        self.assertFalse(summary['published'])                              # 135 funds is far below the floor: nothing is written
        self.assertIn('only', summary['refused'][0])
        self.assertEqual(s3.puts, [])
        # the months brought the per-fund files (which stop on 2026-11-09) up to the newest day
        self.assertEqual(doc['as_of'], '2026-11-24')
        self.assertEqual(doc['quality']['by_date_months'], ['2026-10', '2026-11'])
        whole = {t: M.merge_rows([M.compact(x) for x in f['rows']]) for t, f in CUT['funds'].items()}
        i = fdoc['cols'].index
        row = {r[0]: r for r in fdoc['rows']}
        self.assertAlmostEqual(row['VOO'][i('w1')], sum(r[3] for r in whole['VOO'][-5:]), places=1)
        self.assertAlmostEqual(row['VOO'][i('m3')], sum(r[3] for r in whole['VOO'][-63:]), places=1)

    def test_publishes_the_main_document_last_and_dry_run_writes_nothing(self):
        old = M.MIN_FUNDS
        M.MIN_FUNDS = 50
        try:
            s3 = FakeS3(warehouse())
            summary, _ = M.run(s3, 'b', NOW, dry_run=True, log=lambda *_: None)
            self.assertEqual((summary['published'], summary['refused'], s3.puts), (False, [], []))
            summary, (doc, _, _, shards) = M.run(s3, 'b', NOW, dry_run=False, log=lambda *_: None)
            self.assertTrue(summary['published'])
            keys = [k for k, _ in s3.puts]
            self.assertEqual(keys[-3:], [M.OUT_HISTORY, M.OUT_FUNDS, M.OUT])
            self.assertEqual(len(keys), len(shards) + 3)
            self.assertTrue(all(k.startswith(M.OUT_SHARDS) for k in keys[:-3]))
            self.assertTrue(all(kw.get('ContentEncoding') == 'gzip' and kw.get('ContentType') == 'application/json' for _, kw in s3.puts))
            self.assertEqual(json.loads(gzip.decompress(s3.store[M.OUT]))['as_of'], doc['as_of'])
        finally:
            M.MIN_FUNDS = old

    def test_guard_refuses_an_old_day(self):
        doc = json.loads(M.encode(DOC))
        doc['quality'].update(funds_current=3000, newest_day_age_days=9)
        self.assertEqual(M.guard(doc), ['newest reporting day 2026-11-24 is 9 days old'])
        doc['quality'].update(newest_day_age_days=2, by_date_months=[])
        self.assertEqual(M.guard(doc), ['no by-date month in the warehouse'])
        doc['quality'].update(by_date_months=['2026-11'])
        self.assertEqual(M.guard(doc), [])


if __name__ == '__main__':
    unittest.main()
