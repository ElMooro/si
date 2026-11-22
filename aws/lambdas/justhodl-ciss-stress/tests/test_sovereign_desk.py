"""The sovereign-stress desk document: parsing, measures, compile from originals, fallback, publication."""
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
import hashlib
import io
import json
import sys
import unittest
import urllib.error
from pathlib import Path

HERE = Path(__file__).resolve()
sys.path[:0] = [str(HERE.parents[1] / 'source'), str(HERE.parents[3] / 'shared')]

import ciss_source_model as model          # noqa: E402
import ciss_source_store as store          # noqa: E402
import sovereign_desk as desk              # noqa: E402
from evidence_store import capture         # noqa: E402

NOW = '2026-11-22T07:20:00+00:00'
TODAY = '2026-11-22'


class StorageError(Exception):
    def __init__(self, code):
        self.response = {'Error': {'Code': code}}


class MemoryS3:
    def __init__(self):
        self.objects, self.metadata, self.puts = {}, {}, []

    def get_object(self, Bucket, Key):
        if Key not in self.objects:
            raise StorageError('NoSuchKey')
        body = self.objects[Key]
        return {'Body': io.BytesIO(body), 'ETag': hashlib.sha256(body).hexdigest(), 'Metadata': self.metadata.get(Key, {})}

    def put_object(self, Bucket, Key, Body, **kw):
        if kw.get('IfNoneMatch') == '*' and Key in self.objects:
            raise StorageError('412')
        self.objects[Key] = Body
        self.metadata[Key] = kw.get('Metadata', {})
        self.puts.append(Key)


def business_days(first, last):
    day, out = date.fromisoformat(first), []
    while day <= date.fromisoformat(last):
        if day.weekday() < 5:
            out.append(day.isoformat())
        day += timedelta(days=1)
    return out


def months(first, last):
    (y, m), out = map(int, first.split('-')), []
    while '%04d-%02d' % (y, m) <= last:
        out.append('%04d-%02d' % (y, m))
        y, m = (y + 1, 1) if m == 12 else (y, m + 1)
    return out


def ecb_csv(series, extra=('REF_AREA',)):
    """An ECB CSV response: {key: {'unit', 'mult', 'title', 'area', 'rows': [(period, value text, status)]}}."""
    lines = ['KEY,REF_AREA,TIME_PERIOD,OBS_VALUE,OBS_STATUS,TITLE,UNIT,UNIT_MULT']
    for key, s in series.items():
        for period, value, status in s['rows']:
            lines.append('%s,%s,%s,%s,%s,"%s",%s,%s' % (key, s.get('area', ''), period, value, status, s.get('title', key), s.get('unit', 'PURE_NUMB'), s.get('mult', '0')))
    return ('\n'.join(lines) + '\n').encode()


def level(day, base, bump=0.0):
    """A smooth index with a 2011 hump, so percentiles and peaks have something to find."""
    year = int(day[:4])
    return round(base + (0.5 + bump if year == 2011 else 0.0) + (int(day[8:10]) % 7) * 0.001, 6)


DAYS = business_days('2011-06-01', '2011-12-30') + business_days('2025-10-01', '2026-11-19')
SOV = {'DE': 0.03, 'FR': 0.17, 'IT': 0.05}


def ciss_originals():
    """Warehouse series -> original CSV, as the warehouse retains them (one series per response)."""
    out = {}
    for area, base in SOV.items():
        key = 'CISS.D.%s.Z0Z.4F.EC.SOV_CIN.IDX' % area
        out[key] = ecb_csv({key: {'area': area, 'rows': [(d, level(d, base, 0.3 if area == 'IT' else 0.0), 'A') for d in DAYS]}})
    key = 'CISS.D.U2.Z0Z.4F.EC.SOV_GDPWN.IDX'
    out[key] = ecb_csv({key: {'area': 'U2', 'rows': [(d, level(d, 0.02), 'A') for d in DAYS]}})
    key = model.HEAD
    out[key] = ecb_csv({key: {'area': 'U2', 'rows': [(d, level(d, 0.01), 'A') for d in DAYS]}})
    key = 'CISS.D.DE.Z0Z.4F.EC.SS_CIN.IDX'                              # the ECB printed a blank row for the newest day
    out[key] = ecb_csv({key: {'area': 'DE', 'rows': [(d, level(d, 0.008), 'A') for d in DAYS] + [('2026-11-20', '', 'A')]}})
    key = 'CISS.D.U2.Z0Z.4F.EC.SS_BMN.CON'
    out[key] = ecb_csv({key: {'area': 'U2', 'rows': [(d, level(d, 0.002), 'A') for d in DAYS]}})
    key = 'CLIFS.M.PL._Z.4F.EC.CLIFS_CI.IDX'
    out[key] = ecb_csv({key: {'area': 'PL', 'rows': [(m, '0.05', 'A') for m in months('2020-01', '2026-09')]}})
    key = 'CLIFS.M.GB._Z.4F.EC.CLIFS_CI.IDX'                            # ended at source long ago
    out[key] = ecb_csv({key: {'area': 'GB', 'rows': [(m, '0.07', 'A') for m in months('2008-01', '2010-12')]}})
    key = 'CISS.M.FR.Z0Z.4F.EC.SOV_CI.IDX'                              # legacy monthly series of a country that has a daily one: not used
    out[key] = ecb_csv({key: {'area': 'FR', 'rows': [(m, '0.2', 'A') for m in months('2020-01', '2025-04')]}})
    key = 'CISS.M.PL.Z0Z.4F.EC.SOV_CI.IDX'                              # Poland has no daily SovCISS: its ended monthly series is kept for its history
    out[key] = ecb_csv({key: {'area': 'PL', 'rows': [(m, '0.61' if m == '2011-11' else '0.12', 'A') for m in months('2000-09', '2025-04')]}})
    key = 'CISS.M.U2.Z0Z.4F.EC.SOV_GDPW.IDX'                            # the ended euro-area monthly aggregate: not on the desk
    out[key] = ecb_csv({key: {'area': 'U2', 'rows': [(m, '0.1', 'A') for m in months('2000-09', '2025-04')]}})
    key = 'CISS.D.US.Z0Z.4F.EC.SS_CIN.IDX'
    out[key] = ecb_csv({key: {'area': 'US', 'rows': [(d, level(d, 0.014), 'A') for d in business_days('1987-10-01', '1987-12-31') + DAYS]}})
    return out


def aux_originals():
    yc = lambda code, tenor: 'YC.B.U2.EUR.4F.%s.SV_C_YM.SR_%s' % (code, tenor)
    recent = business_days('2026-10-01', '2026-11-19')
    curve = {yc('G_N_A', '10Y'): {'area': 'U2', 'unit': 'PCPA', 'rows': [(d, '3.4415130227', 'A') for d in recent]},
             yc('G_N_C', '10Y'): {'area': 'U2', 'unit': 'PCPA', 'rows': [(d, '4.0395926696', 'A') for d in recent]},
             yc('G_N_A', '2Y'): {'area': 'U2', 'unit': 'PCPA', 'rows': [(d, '3.1250637519', 'A') for d in recent]},
             yc('G_N_C', '2Y'): {'area': 'U2', 'unit': 'PCPA', 'rows': [(d, '3.2928774018', 'A') for d in recent[:-1]]}}      # one day short: no spread that day
    irs = lambda area, cur: 'IRS.M.%s.L.L40.CI.0000.%s.N.Z' % (area, cur)
    span = months('2010-01', '2026-10')
    yields = {irs('DE', 'EUR'): {'area': 'DE', 'unit': 'PC', 'rows': [(m, '3.25', 'A') for m in span]},
              irs('FR', 'EUR'): {'area': 'FR', 'unit': 'PC', 'rows': [(m, '4.10' if m >= '2026-01' else '3.60', 'A') for m in span]},
              irs('IT', 'EUR'): {'area': 'IT', 'unit': 'PC', 'rows': [(m, '7.06' if m == '2011-11' else '4.00', 'A') for m in span]},
              irs('BG', 'BGN'): {'area': 'BG', 'unit': 'PC', 'rows': [(m, '3.93', 'A') for m in months('2010-01', '2025-12')]},
              irs('BG', 'EUR'): {'area': 'BG', 'unit': 'PC', 'rows': [(m, '4.09', 'A') for m in months('2026-01', '2026-10')]},
              irs('DK', 'DKK'): {'area': 'DK', 'unit': 'PC', 'rows': [(m, '3.00', 'A') for m in span]},
              irs('U2', 'EUR'): {'area': 'U2', 'unit': 'PC', 'rows': [(m, '4.0771', 'A') for m in span]},
              irs('V5', 'Z01'): {'area': 'V5', 'unit': 'PC', 'rows': [(m, '4.33', 'A') for m in span]},
              irs('GB', 'GBP'): {'area': 'GB', 'unit': 'PC', 'rows': [(m, '1.00', 'A') for m in months('2010-01', '2020-01')]}}
    tgb = lambda area, sector: 'TGB.M.%s.%s.A094T.U2.EUR.E' % (area, sector)
    target = {tgb('DE', 'N'): {'area': 'DE', 'unit': 'EUR', 'mult': '6', 'rows': [(m, '1000000.6', 'A') for m in months('2024-01', '2026-09')]},
              tgb('IT', 'N'): {'area': 'IT', 'unit': 'EUR', 'mult': '6', 'rows': [(m, '-400000', 'A') for m in months('2024-01', '2026-09')]},
              tgb('4F', 'E'): {'area': '4F', 'unit': 'EUR', 'mult': '6', 'rows': [(m, '-267886.65', 'A') for m in months('2024-01', '2026-09')]},
              tgb('U4', 'N'): {'area': 'U4', 'unit': 'EUR', 'mult': '6', 'rows': [(m, '683.61', 'A') for m in months('2024-01', '2026-09')]}}
    gfs = lambda area: 'GFS.Q.N.%s.W0.S13.S1.C.L.LE.GD.T._Z.XDC_R_B1GQ_CY._T.F.V.N._T' % area
    quarters = ['%d-Q%d' % (y, q) for y in range(2022, 2027) for q in range(1, 5) if (y, q) <= (2026, 2)]
    debt = {gfs('FR'): {'area': 'FR', 'unit': 'XDC_R_B1GQ_CY', 'rows': [(q, '%.1f' % (110 + i * 0.5), 'A') for i, q in enumerate(quarters)]},
            gfs('B6'): {'area': 'B6', 'unit': 'XDC_R_B1GQ_CY', 'rows': [(q, '82.9', 'A') for q in quarters]}}
    fiscal = lambda code: 'GFS.Q.N.%%s.W0.S13.S1.%s.XDC_R_B1GQ_CY._Z.S.V.CY._T' % code
    deficit = {fiscal('_Z.B.B9._Z._Z._Z') % 'FR': {'area': 'FR', 'unit': 'XDC_R_B1GQ_CY', 'rows': [(q, '-5.1066', 'A') for q in quarters]},
               fiscal('_Z.B.B9._Z._Z._Z') % 'I9': {'area': 'I9', 'unit': 'XDC_R_B1GQ_CY', 'rows': [(q, '-3.2404', 'A') for q in quarters]},
               fiscal('_Z.B.B9._Z._Z._Z') % 'B6': {'area': 'B6', 'unit': 'XDC_R_B1GQ_CY', 'rows': [(q, '-3.1', 'A') for q in quarters]}}
    primary = {fiscal('_Z.B.B9P._Z._Z._Z') % 'FR': {'area': 'FR', 'unit': 'XDC_R_B1GQ_CY', 'rows': [(q, '-2.6964', 'A') for q in quarters]}}
    interest = {fiscal('C.D.D41._Z._Z._T') % 'FR': {'area': 'FR', 'unit': 'XDC_R_B1GQ_CY', 'rows': [(q, '2.4102', 'A') for q in quarters]}}
    eurusd = {'EXR.D.USD.EUR.SP00.A': {'unit': 'USD', 'rows': [(d, '1.0992', 'A') for d in recent] + [('2026-11-20', '1.0990', 'A')]}}
    weeks = ['2026-W%02d' % w for w in range(30, 47)]
    claims_ea = {'ILM.W.U2.C.A030000.U2.Z06': {'area': 'U2', 'unit': 'EUR', 'mult': '6', 'rows': [(w, '20193', 'A') for w in weeks]}}
    claims_non = {'ILM.W.U2.C.A020000.U4.Z06': {'area': 'U2', 'unit': 'EUR', 'mult': '6', 'rows': [(w, '521892', 'A') for w in weeks]}}
    return {'curve': ecb_csv(curve), 'yields': ecb_csv(yields), 'target': ecb_csv(target), 'debt': ecb_csv(debt), 'deficit': ecb_csv(deficit), 'primary': ecb_csv(primary),
            'interest': ecb_csv(interest), 'eurusd': ecb_csv(eurusd),
            'fx_claims_ea': ecb_csv(claims_ea), 'fx_claims_nonea': ecb_csv(claims_non)}


def staged(drop=()):
    """Originals stored through the evidence store; (client, binding, receipts, aux)."""
    s3 = MemoryS3()
    stamp = datetime.fromisoformat(NOW)
    receipts = {}
    for key, raw in ciss_originals().items():
        receipts[key] = capture(s3, 'test', 'ecb', store.BASE + key.replace('.', '/', 1) + '?format=csvdata', raw, stamp)
    aux = {}
    for name, raw in aux_originals().items():
        if name in drop:
            continue
        url = desk.BASE + desk.AUX[name] + '?format=csvdata'
        aux[name] = {'evidence': capture(s3, 'test', 'ecb', url, raw, stamp), 'acquired_at': NOW, 'request_url': url}
    binding = {'generated_at': '2026-11-22T07:13:48+00:00', 'manifest_key': model.PREFIX + 'runs/abc.json', 'output_sha256': 'f' * 64}
    return s3, binding, receipts, aux


def compiled(drop=()):
    s3, binding, receipts, aux = staged(drop)
    read = lambda receipt: store.original(s3, 'test', receipt)
    return desk.compile_desk(binding, receipts, aux, read, NOW, {name: 'HTTP_503' for name in drop})


class Parsing(unittest.TestCase):
    def test_a_period_ends_on_its_last_day(self):
        self.assertEqual(desk.period_end('2026-11-19'), date(2026, 11, 19))
        self.assertEqual(desk.period_end('2024-02'), date(2024, 2, 29))
        self.assertEqual(desk.period_end('2026-Q2'), date(2026, 6, 30))
        self.assertEqual(desk.period_end('2026-W46'), date(2026, 11, 13))      # the Friday the weekly statement is drawn up for
        self.assertRaises(ValueError, desk.period_end, '2026')

    def test_a_blank_observation_is_kept_and_an_error_page_is_refused(self):
        raw = ecb_csv({'X.D.A': {'rows': [('2026-11-18', '1.5', 'A'), ('2026-11-19', '', 'A')]}})
        rows = desk.flow_series(raw)['X.D.A']['rows']
        self.assertEqual([(p['period'], p['value']) for p in rows], [('2026-11-18', Decimal('1.5')), ('2026-11-19', None)])
        self.assertRaises(ValueError, desk.flow_series, b'<html><body>Service unavailable</body></html>')
        self.assertRaises(ValueError, desk.flow_series, b'KEY,TIME_PERIOD,OBS_VALUE\n')
        clash = ecb_csv({'X.D.A': {'rows': [('2026-11-18', '1.5', 'A'), ('2026-11-18', '1.6', 'A')]}})
        self.assertRaises(ValueError, desk.flow_series, clash)

    def test_only_usable_rows_up_to_today_count_and_a_blank_newest_row_is_reported(self):
        raw = ecb_csv({'X.D.A': {'mult': '6', 'rows': [('2026-11-17', '100', 'A'), ('2026-11-18', '200', 'F'), ('2026-11-19', '300', 'P'), ('2026-11-20', '', 'A'), ('2026-12-01', '9', 'A')]}})
        entry = desk.flow_series(raw)['X.D.A']
        obs, info = desk.observations(entry, TODAY, Decimal(10) ** (6 - 9))
        self.assertEqual(obs, [('2026-11-17', Decimal('0.1')), ('2026-11-19', Decimal('0.3'))])
        self.assertEqual((info['blank_after'], info['left_out'], info['exact'], info['last_row']), ('2026-11-20', {'F': 1}, '300', 2))


class Measures(unittest.TestCase):
    def test_changes_use_the_reading_the_source_has_one_horizon_back(self):
        obs = [(d, Decimal(str(i))) for i, d in enumerate(business_days('2025-11-03', '2026-11-19'))]
        index = dict(obs)
        last = obs[-1][1]
        self.assertEqual(desk.change_over(obs, index, 'D', 7, 0), last - index['2026-11-12'])
        self.assertEqual(desk.change_over(obs, index, 'D', 0, 1), last - index['2026-10-19'])
        self.assertEqual(desk.change_over(obs, index, 'D', 0, 12), last - index['2025-11-19'])
        sunday = [(d, v) for d, v in obs if d <= '2026-11-16']                 # a Monday: one month back is a Friday's reading
        self.assertEqual(desk.change_over(sunday, dict(sunday), 'D', 0, 1), sunday[-1][1] - dict(sunday)['2026-10-16'])
        short = obs[-3:]
        self.assertIsNone(desk.change_over(short, dict(short), 'D', 0, 12))
        monthly = [(desk.period_end(m).isoformat(), Decimal(i)) for i, m in enumerate(months('2025-01', '2026-10'))]
        self.assertEqual(desk.change_over(monthly, dict(monthly), 'M', 0, 12), Decimal(12))
        self.assertEqual(desk.change_over(monthly, dict(monthly), 'M', 0, 1), Decimal(1))
        self.assertIsNone(desk.change_over(monthly, dict(monthly), 'M', 7, 0))
        quarterly = [(desk.period_end(q).isoformat(), Decimal(i)) for i, q in enumerate(['2025-Q1', '2025-Q2', '2025-Q3', '2025-Q4', '2026-Q1', '2026-Q2'])]
        self.assertEqual(desk.change_over(quarterly, dict(quarterly), 'Q', 0, 3), Decimal(1))
        self.assertEqual(desk.change_over(quarterly, dict(quarterly), 'Q', 0, 12), Decimal(4))
        self.assertIsNone(desk.change_over(quarterly, dict(quarterly), 'Q', 0, 1))

    def test_a_measure_carries_its_place_in_history_and_its_peak_inside_a_named_window(self):
        obs = [(d, Decimal(str(level(d, 0.05, 0.3)))) for d in DAYS]
        m = desk.measure('SOV_IT', 'Italy SovCISS', 'index', 'D', obs, TODAY, {'exact': '0.055', 'last_row': 9})
        self.assertEqual((m['date'], m['status'], m['age_days']), ('2026-11-19', 'current', 3))
        self.assertAlmostEqual(m['value'], level('2026-11-19', 0.05), places=5)
        plain = sorted(float(v) for _, v in obs)
        self.assertAlmostEqual(m['pctile'], round(100 * sum(v <= float(obs[-1][1]) for v in plain) / len(plain), 1))
        hump = max((float(v), d) for d, v in obs if '2011-07-01' <= d <= '2011-12-31')
        self.assertAlmostEqual(m['peaks']['italy2011']['hi'][0], hump[0], places=5)
        self.assertNotIn('covid2020', m['peaks'])                                # no observation inside that window
        self.assertEqual(m['high'][0], m['tops'][0][0])
        gaps = [abs((date.fromisoformat(a[1]) - date.fromisoformat(b[1])).days) for i, a in enumerate(m['tops']) for b in m['tops'][:i]]
        self.assertTrue(all(g >= 120 for g in gaps))
        self.assertEqual(len(m['spark']), 260)

    def test_the_five_year_window_runs_back_from_the_day_of_compilation(self):
        obs = [(desk.period_end(m).isoformat(), Decimal(i)) for i, m in enumerate(months('2015-01', '2026-09'))]
        m = desk.measure('X', 'x', 'index', 'M', obs, TODAY)
        self.assertEqual(m['pctile_5y'], 100.0)
        falling = [(d, -v) for d, v in obs]
        window = [v for d, v in falling if d >= '2021-11-22']                    # 59 month-ends, as the warehouse counts them
        self.assertEqual((len(window), desk.measure('X', 'x', 'index', 'M', falling, TODAY)['pctile_5y']), (59, round(100 / 59, 1)))

    def test_a_reading_older_than_its_rhythm_is_late_and_a_long_silent_series_has_ended(self):
        late = desk.measure('X', 'x', 'pct', 'D', [('2026-11-01', Decimal(1))], TODAY)
        ended = desk.measure('X', 'x', 'pct', 'M', [('2025-04-30', Decimal(1))], TODAY)
        self.assertEqual((late['status'], ended['status']), ('late', 'ended'))
        self.assertIsNone(desk.measure('X', 'x', 'pct', 'D', [], TODAY))

    def test_weekly_keeps_the_last_reading_of_each_iso_week(self):
        obs = [('2025-12-29', Decimal(1)), ('2025-12-31', Decimal(2)), ('2026-01-02', Decimal(3)), ('2026-01-05', Decimal(4))]
        self.assertEqual(desk.weekly(obs), [('2026-01-02', Decimal(3)), ('2026-01-05', Decimal(4))])


class Compile(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.doc, cls.hist = compiled()

    def test_every_block_is_there_and_nothing_is_reported_wrong(self):
        doc = self.doc
        self.assertEqual((doc['contract'], doc['errors'], doc['coverage']['status']), (desk.CONTRACT, {}, 'measured'))
        for mid in ('SOV_DE', 'SOV_FR', 'SOV_IT', 'EA_SOV_GDP', 'EA_SOV_RANGE', 'EA_CISS', 'EA_CON_BMN', 'CISS_DE', 'CLIFS_PL', 'EA_AAA_10Y', 'EA_ALL_10Y', 'EA_CURVE_10Y',
                    'EA_CURVE_2Y', 'Y10_DE', 'Y10_FR', 'S10_FR', 'S10_IT', 'Y10_U2', 'TGT_DE', 'TGT_IT', 'TGT_4F', 'DEBT_FR', 'AUX_EURUSD', 'AUX_FX_EA', 'AUX_FX_NONEA'):
            self.assertIn(mid, doc['measures'], mid)
            self.assertEqual(len(self.hist['series'][mid]['d']), len(self.hist['series'][mid]['v']), mid)
        self.assertEqual(doc['auxiliaries'], ['AUX_EURUSD', 'AUX_FX_EA', 'AUX_FX_NONEA'])
        self.assertEqual({s['name'] for s in doc['sources']}, {'ciss', *desk.AUX})

    def test_the_cross_section_is_the_same_day_for_every_country(self):
        cross, m = self.doc['cross_section'], self.doc['measures']
        self.assertEqual((cross['date'], cross['n'], [r['area'] for r in cross['ranking']]), ('2026-11-19', 3, ['FR', 'IT', 'DE']))
        self.assertAlmostEqual(cross['range'], m['SOV_FR']['value'] - m['SOV_DE']['value'], places=5)
        self.assertAlmostEqual(m['EA_SOV_RANGE']['value'], cross['range'], places=5)
        self.assertAlmostEqual(m['EA_SOV_RANGE']['peaks']['italy2011']['hi'][0], (0.05 + 0.8) - 0.53, places=2)

    def test_spreads_are_exact_same_date_differences(self):
        m = self.doc['measures']
        self.assertEqual(m['EA_CURVE_10Y']['value'], round((4.0395926696 - 3.4415130227) * 100, 2))
        self.assertEqual((m['EA_CURVE_10Y']['date'], m['EA_CURVE_2Y']['date']), ('2026-11-19', '2026-11-18'))     # no 2-year all-issuer rate on the 19th
        self.assertEqual((m['S10_FR']['value'], m['S10_FR']['unit'], m['S10_FR']['date']), (85.0, 'bp', '2026-10-31'))
        self.assertEqual(m['S10_FR']['change']['12m'], 50.0)                     # 85 bp now against 35 bp in October 2025
        self.assertEqual(m['S10_IT']['peaks']['italy2011']['hi'], [381.0, '2011-11-30'])
        self.assertEqual(m['Y10_IT']['high'], [7.06, '2011-11-30'])

    def test_target_and_claims_are_in_billions_of_euro(self):
        m = self.doc['measures']
        self.assertEqual((m['TGT_4F']['value'], m['TGT_DE']['value'], m['TGT_IT']['value']), (-267.887, 1000.001, -400.0))
        self.assertEqual((m['AUX_FX_EA']['value'], m['AUX_FX_EA']['date'], m['AUX_FX_NONEA']['value']), (20.193, '2026-11-13', 521.892))
        self.assertEqual((m['AUX_EURUSD']['value'], m['AUX_EURUSD']['date'], m['AUX_EURUSD']['exact']), (1.099, '2026-11-20', '1.0990'))
        self.assertNotIn('TGT_U4', m)
        # what the national central banks in deficit owe: Italy's 400 bn; the ECB's own balance is not a national central bank's
        self.assertEqual((m['EA_TGT_DEBT']['value'], m['EA_TGT_DEBT']['date'], m['EA_TGT_DEBT']['n']), (400.0, '2026-09-30', 33))

    def test_a_country_that_changed_currency_is_read_from_its_current_series(self):
        m = self.doc['measures']
        self.assertEqual((m['Y10_BG']['key'], m['Y10_BG']['value']), ('IRS.M.BG.L.L40.CI.0000.EUR.N.Z', 4.09))
        rows = {c['area']: c for c in self.doc['countries']}
        self.assertEqual((rows['BG']['euro'], rows['DK']['euro'], rows['FR']['euro']), (True, False, True))
        self.assertEqual(rows['FR']['ids'], {'sovciss': 'SOV_FR', 'yield': 'Y10_FR', 'spread': 'S10_FR', 'debt': 'DEBT_FR', 'deficit': 'DEF_FR', 'primary': 'PRIM_FR', 'interest': 'INT_FR'})
        self.assertNotIn('Y10_V5', m)

    def test_ended_and_legacy_series_do_not_pose_as_current(self):
        m = self.doc['measures']
        self.assertNotIn('CLIFS_GB', m)
        self.assertNotIn('Y10_GB', m)
        ended = {e['id'] for e in self.doc['ended_at_source']}
        self.assertEqual(ended, {'CLIFS_GB', 'Y10_GB'})
        self.assertEqual([k['id'] for k in m.values() if k['key'] and '.SOV_CI.' in k['key']], ['SOVM_PL'])

    def test_a_country_with_only_the_ended_monthly_sovciss_keeps_its_history(self):
        m = self.doc['measures']['SOVM_PL']
        self.assertEqual((m['status'], m['group'], m['date'], m['value'], m['freq']), ('ended', 'sovciss_ended', '2025-04-30', 0.12, 'M'))
        self.assertEqual(m['peaks']['italy2011']['hi'], [0.61, '2011-11-30'])
        self.assertEqual(self.hist['series']['SOVM_PL']['d'][0], '2000-09-30')
        rows = {c['area']: c for c in self.doc['countries']}
        self.assertEqual(rows['PL']['ids'].get('sovciss_ended'), 'SOVM_PL')
        self.assertNotIn('sovciss_ended', rows['FR']['ids'])                     # France is read from its daily series
        self.assertNotIn('SOVM_FR', self.doc['measures'])
        self.assertNotIn('SOVM_PL', {e['id'] for e in self.doc['ended_at_source']})
        self.assertFalse([k for k in self.doc['measures'] if k.startswith('SOVM_U2') or k == 'EA_SOVM'])

    def test_the_fiscal_figures_are_the_ecbs_four_quarter_sums(self):
        m = self.doc['measures']
        self.assertEqual((m['DEF_FR']['value'], m['PRIM_FR']['value'], m['INT_FR']['value']), (-5.11, -2.7, 2.41))
        self.assertEqual((m['DEF_FR']['unit'], m['DEF_FR']['freq'], m['DEF_FR']['date'], m['DEF_FR']['group']), ('pct_gdp', 'Q', '2026-06-30', 'deficit'))
        self.assertEqual(m['DEF_I9']['label'], 'Euro area government balance (deficit is negative)')
        self.assertNotIn('DEF_B6', m)

    def test_the_ciss_of_the_united_states_reaches_the_1987_window(self):
        m = self.doc['measures']['CISS_US']
        self.assertEqual(self.doc['world_ciss'], ['CISS_US'])
        self.assertEqual((m['start'], m['group']), ('1987-10-01', 'ciss'))
        self.assertIn('crash1987', m['peaks'])
        self.assertNotIn('crash1987', self.doc['measures']['SOV_FR']['peaks'])

    def test_the_windows_are_in_order_and_do_not_overlap(self):
        eps = self.doc['episodes']
        self.assertEqual(len(eps), 22)
        self.assertEqual(len({e['id'] for e in eps}), 22)
        for a, b in zip(eps, eps[1:]):
            self.assertLess(a['to'], b['from'], (a['id'], b['id']))
        for e in eps:
            self.assertLess(e['from'], e['to'])

    def test_a_blank_newest_row_leaves_the_last_reading_with_its_own_date(self):
        m = self.doc['measures']['CISS_DE']
        self.assertEqual((m['date'], m['blank_after'], m['status']), ('2026-11-19', '2026-11-20', 'current'))

    def test_every_measure_names_the_original_it_was_read_from(self):
        for mid, m in self.doc['measures'].items():
            self.assertTrue(m['evidence'] or m['derived'], mid)
            if m['evidence']:
                self.assertEqual((m['evidence']['contract'], m['evidence']['captured']), ('source-evidence.v1', True), mid)

    def test_the_same_originals_and_clock_give_the_same_bytes(self):
        again, again_hist = compiled()
        self.assertEqual((model.digest(again), model.digest(again_hist)), (model.digest(self.doc), model.digest(self.hist)))

    def test_a_refused_response_costs_only_its_own_block(self):
        doc, _ = compiled(drop=('target',))
        self.assertEqual((doc['errors'], doc['coverage']['status']), ({'target': 'HTTP_503'}, 'degraded'))
        self.assertFalse([k for k in doc['measures'] if k.startswith('TGT_')])
        self.assertIn('S10_FR', doc['measures'])


class FakeStore:
    """The store's network and readback, with one path refused."""
    def __init__(self, refuse=()):
        self.refuse, self.asked = set(refuse), []
        self.safe_error, self.original, self.immutable, self.read, self.missing = store.safe_error, store.original, store.immutable, store.read, store.missing

    def request(self, url, deadline):
        self.asked.append(url)
        name = next(n for n, path in desk.AUX.items() if url == desk.BASE + path + '?format=csvdata')
        if name in self.refuse:
            raise urllib.error.HTTPError(url, 503, 'Service Unavailable', {}, None)
        return aux_originals()[name], datetime.fromisoformat(NOW)


class Publication(unittest.TestCase):
    def test_one_refusal_falls_back_to_the_last_retained_response(self):
        s3, _, _, aux = staged()
        previous = {'sources': [{'name': 'target', 'path': desk.AUX['target'], 'evidence': aux['target']['evidence'], 'acquired_at': '2026-11-21T07:15:00+00:00'}]}
        pauses = []
        got, errors = desk.gather(s3, 'test', FakeStore(refuse=('target', 'debt')), 10 ** 9, previous, sleep=pauses.append)
        self.assertEqual(set(got), set(desk.AUX) - {'debt'})
        self.assertTrue(got['target']['carried'])
        self.assertEqual(got['target']['evidence'], aux['target']['evidence'])
        self.assertEqual(errors, {'target': 'HTTP_503 - the last retained response of 2026-11-21 is used', 'debt': 'HTTP_503'})
        self.assertEqual(len(pauses), len(desk.AUX) - 1)

    def test_run_publishes_both_documents_and_a_receipt_that_reproduces_them(self):
        s3, binding, receipts, _ = staged(drop=tuple(desk.AUX))
        warehouse = {'contract': model.CONTRACT, 'generated_at': binding['generated_at'], 'replay': {'manifest_key': binding['manifest_key'], 'output_sha256': binding['output_sha256']},
                     'series': [{'key': key, 'area': key.split('.')[2], 'evidence': receipt} for key, receipt in receipts.items()]}
        s3.objects[desk.WAREHOUSE] = model.encoded(warehouse)
        result = desk.run(s3, 'test', FakeStore(), now=datetime.fromisoformat(NOW))
        self.assertTrue(result['published'])
        doc, hist = json.loads(s3.objects[desk.CURRENT]), json.loads(s3.objects[desk.HISTORY])
        manifest = json.loads(s3.objects[doc['replay']['manifest_key']])
        self.assertEqual(doc['replay']['manifest_key'], desk.RUNS + model.digest(manifest) + '.json')
        self.assertEqual(model.digest({k: v for k, v in doc.items() if k not in ('replay', 'history')}), manifest['output_sha256'])
        self.assertEqual((model.digest(hist), doc['history']), (manifest['history_sha256'], {'key': desk.HISTORY, 'sha256': manifest['history_sha256']}))
        self.assertEqual(s3.objects[manifest['compiler']['key']], Path(desk.__file__).read_bytes())
        self.assertEqual(doc['warehouse'], binding)
        self.assertNotIn('CISS.M.FR.Z0Z.4F.EC.SOV_CI.IDX', manifest['receipts'])
        self.assertNotIn('CISS.M.U2.Z0Z.4F.EC.SOV_GDPW.IDX', manifest['receipts'])
        self.assertIn('CISS.M.PL.Z0Z.4F.EC.SOV_CI.IDX', manifest['receipts'])
        rebuilt, _ = desk.compile_desk(manifest['warehouse'], manifest['receipts'], manifest['aux'], lambda r: store.original(s3, 'test', r), manifest['generated_at'], manifest['errors'])
        self.assertEqual(model.digest(rebuilt), manifest['output_sha256'])
        self.assertLess(s3.puts.index(desk.HISTORY), s3.puts.index(desk.CURRENT))      # the page never reads a document whose history is not there yet

    def test_run_refuses_a_warehouse_without_its_contract(self):
        s3 = MemoryS3()
        s3.objects[desk.WAREHOUSE] = model.encoded({'contract': 'something-else', 'series': []})
        self.assertRaises(ValueError, desk.run, s3, 'test', FakeStore())


class Handler(unittest.TestCase):
    """The Lambda's handler: the desk document follows the warehouse run and can never fail it."""

    def load(self):
        import importlib.util
        import types
        from unittest.mock import patch
        path = HERE.parents[1] / 'source' / 'lambda_function.py'
        with patch.dict(sys.modules, {'boto3': types.SimpleNamespace(client=lambda *a, **k: 'S3')}):
            spec = importlib.util.spec_from_file_location('ciss_stress_handler', path)
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
        return module

    def context(self, seconds):
        import types
        return types.SimpleNamespace(get_remaining_time_in_millis=lambda: seconds * 1000)

    def test_the_desk_follows_the_warehouse_and_its_failure_is_reported_not_raised(self):
        from unittest.mock import patch
        m = self.load()
        with patch.object(m, 'run', return_value={'published': True}) as warehouse, patch.object(m.sovereign_desk, 'run', return_value={'published': True, 'coverage': {}}) as compiled, \
                patch.object(m.world_history, 'run', return_value={'published': True, 'sets': {}}) as world, patch.object(m.dollar_shortage, 'run', return_value={'published': True, 'latest': {}}) as dollar:
            body = json.loads(m.lambda_handler({}, self.context(900))['body'])
        warehouse.assert_called_once_with('S3', m.BUCKET, budget_seconds=480)
        compiled.assert_called_once_with('S3', m.BUCKET, m.store, budget_seconds=780)
        dollar.assert_called_once_with('S3', m.BUCKET, m.store)
        world.assert_called_once_with('S3', m.BUCKET, budget_seconds=240)
        self.assertEqual(body, {'published': True, 'sovereign': {'published': True, 'coverage': {}}, 'dollar': {'published': True, 'latest': {}}, 'world': {'published': True, 'sets': {}}})
        self.assertEqual(list(body), ['published', 'sovereign', 'dollar', 'world'])            # the dollar document reads the desk document, so it follows it
        with patch.object(m, 'run', return_value={'published': True}), patch.object(m.sovereign_desk, 'run', side_effect=ValueError('boom')), \
                patch.object(m.world_history, 'run', side_effect=KeyError('gone')), patch.object(m.dollar_shortage, 'run', side_effect=ValueError('differs')):
            body = json.loads(m.lambda_handler({}, self.context(900))['body'])
        self.assertEqual(body, {'published': True, 'sovereign': {'published': False, 'error': 'ValueError'}, 'dollar': {'published': False, 'error': 'ValueError'},
                                'world': {'published': False, 'error': 'KeyError'}})

    def test_with_little_time_left_the_last_document_stays(self):
        from unittest.mock import patch
        m = self.load()
        with patch.object(m, 'run', return_value={'published': True}), patch.object(m.sovereign_desk, 'run') as compiled, patch.object(m.world_history, 'run', return_value={'published': True}) as world, \
                patch.object(m.dollar_shortage, 'run', return_value={'published': True}):
            body = json.loads(m.lambda_handler({}, self.context(120))['body'])
        compiled.assert_not_called()
        self.assertFalse(body['sovereign']['published'])
        self.assertIn('not enough time', body['sovereign']['reason'])
        world.assert_called_once_with('S3', m.BUCKET, budget_seconds=80)                        # the World tab's histories need far less time
        with patch.object(m, 'run', return_value={'published': True}), patch.object(m.world_history, 'run') as world, patch.object(m.dollar_shortage, 'run', return_value={'published': True}) as dollar:
            body = json.loads(m.lambda_handler({}, self.context(50))['body'])
        world.assert_not_called()
        self.assertIn('not enough time', body['world']['reason'])
        dollar.assert_called_once()                                                             # thirty seconds are enough for it
        with patch.object(m, 'run', return_value={'published': True}), patch.object(m.dollar_shortage, 'run') as dollar:
            body = json.loads(m.lambda_handler({}, self.context(40))['body'])
        dollar.assert_not_called()
        self.assertIn('not enough time', body['dollar']['reason'])

    def test_the_desk_can_be_recompiled_alone_without_collecting_the_warehouse_again(self):
        from unittest.mock import patch
        m = self.load()
        with patch.object(m, 'run') as warehouse, patch.object(m.sovereign_desk, 'run', return_value={'published': True}) as compiled, patch.object(m.world_history, 'run', return_value={'published': True}) as world, \
                patch.object(m.dollar_shortage, 'run', return_value={'published': True}) as dollar:
            body = json.loads(m.lambda_handler({'sovereign_only': True}, self.context(900))['body'])
        warehouse.assert_not_called()
        compiled.assert_called_once()
        dollar.assert_called_once()
        world.assert_called_once()                                                              # the World tab reads the desk's monthly yields, so it follows
        self.assertEqual(body, {'sovereign': {'published': True}, 'dollar': {'published': True}, 'world': {'published': True}})

    def test_the_world_histories_can_be_compiled_alone_with_no_ecb_request(self):
        from unittest.mock import patch
        m = self.load()
        with patch.object(m, 'run') as warehouse, patch.object(m.sovereign_desk, 'run') as compiled, patch.object(m.world_history, 'run', return_value={'published': True, 'sets': {'yield': 30}}) as world, \
                patch.object(m.dollar_shortage, 'run') as dollar:
            body = json.loads(m.lambda_handler({'world_only': True}, self.context(900))['body'])
        warehouse.assert_not_called()
        compiled.assert_not_called()
        dollar.assert_not_called()
        world.assert_called_once_with('S3', m.BUCKET, budget_seconds=240)
        self.assertEqual(body, {'world': {'published': True, 'sets': {'yield': 30}}})

    def test_the_dollar_shortage_document_can_be_compiled_alone_from_what_is_already_kept(self):
        from unittest.mock import patch
        m = self.load()
        with patch.object(m, 'run') as warehouse, patch.object(m.sovereign_desk, 'run') as compiled, patch.object(m.world_history, 'run') as world, \
                patch.object(m.dollar_shortage, 'run', return_value={'published': True, 'latest': {'AUX_EURUSD': [1.099, '2026-11-20']}}) as dollar:
            body = json.loads(m.lambda_handler({'dollar_only': True}, self.context(900))['body'])
        warehouse.assert_not_called()
        compiled.assert_not_called()
        world.assert_not_called()
        dollar.assert_called_once_with('S3', m.BUCKET, m.store)
        self.assertEqual(body, {'dollar': {'published': True, 'latest': {'AUX_EURUSD': [1.099, '2026-11-20']}}})


if __name__ == '__main__':
    unittest.main(verbosity=2)
