from copy import deepcopy
from datetime import timedelta
from io import StringIO
from unittest.mock import patch
import contextlib, json, unittest, urllib.parse
import cargo_test_harness as h
import cargo_measurements as m


class CalendarTests(unittest.TestCase):
    def fixture(self):
        _, rows, _, opener = h.fixture()
        end = max(m.source_date(r['date']) for r in rows)
        acquisition = {'start': (end-timedelta(days=41)).isoformat(), 'end': end.isoformat()}
        return rows, opener.catalog, acquisition

    def test_exact_two_windows_and_matched_cohorts_keep_missing_ports_visible(self):
        rows, catalog, acquisition = self.fixture()
        result = m.build(rows, catalog, acquisition, h.NOW.isoformat())
        self.assertEqual((result['catalog_ports'], result['port_rows'], result['observation_rows']), (4, 4, 126))
        for port in result['ports'][:3]:
            current = port['legs']['import']['current_7d']; previous = port['legs']['import']['previous_28d']
            self.assertEqual((current['available_days'], previous['available_days']), (7, 28))
            self.assertEqual(current['sum_metric_tons'], sum(10000+i*100 for i in range(35, 42)))
            self.assertEqual(previous['sum_metric_tons'], sum(10000+i*100 for i in range(7, 35)))
        ghost = result['ports'][3]
        self.assertEqual(ghost['observation_rows'], 0); self.assertIsNone(ghost['legs']['import']['current_7d']['mean_metric_tons_per_day'])
        self.assertEqual(result['covered_port_cohort']['legs']['import']['matched_port_ids'], ['p0', 'p1', 'p2'])
        self.assertEqual(result['covered_port_cohort']['legs']['import']['excluded_port_ids'], ['p3'])

    def test_missing_latest_value_is_not_zero_and_each_direction_keeps_its_own_cohort(self):
        rows, catalog, acquisition = self.fixture()
        rows[41]['import'] = None
        result = m.build(rows, catalog, acquisition, h.NOW.isoformat())
        missing = result['ports'][0]['legs']['import']['current_7d']
        self.assertEqual(missing['available_days'], 6); self.assertEqual(missing['missing_or_invalid_dates'], [acquisition['end']])
        self.assertIsNone(missing['mean_metric_tons_per_day'])
        self.assertEqual(result['ports'][0]['last_observation_date'], acquisition['end'])
        self.assertEqual(result['covered_port_cohort']['legs']['import']['matched_ports'], 2)
        self.assertEqual(result['covered_port_cohort']['legs']['export']['matched_ports'], 3)

    def test_zero_is_valid_but_never_a_percentage_denominator(self):
        rows, catalog, acquisition = self.fixture()
        for row in rows: row.update(**{'import': 0, 'export': 0})
        result = m.build(rows, catalog, acquisition, h.NOW.isoformat())
        for direction in ('import', 'export'):
            leg = result['covered_port_cohort']['legs'][direction]
            self.assertEqual(leg['matched_ports'], 3)
            self.assertEqual(leg['current_7d']['sum_metric_tons'], 0)
            self.assertEqual(leg['comparison'], {'delta_metric_tons_per_day': 0.0, 'percent': None, 'status': 'zero_denominator'})

    def test_iso_and_epoch_midnight_identify_the_same_day_invalid_clocks_do_not(self):
        day = h.NOW.replace(hour=0, minute=0)
        for value in (day.date().isoformat(), day.isoformat(), int(day.timestamp()*1000)):
            self.assertEqual(m.source_date(value), day.date())
        for value in (True, False, '2026-02-30', '2026-09-27T00:00:00', '2026-09-27T01:00:00Z', day.timestamp(), float('inf')):
            self.assertIsNone(m.source_date(value))

    def test_duplicates_and_invalid_source_identifiers_cannot_overwrite(self):
        for kind in ('day', 'object', 'catalog', 'date', 'future'):
            rows, catalog, acquisition = self.fixture()
            if kind == 'day': rows.append({**rows[0], 'ObjectId': 200})
            elif kind == 'object': rows[1]['ObjectId'] = rows[0]['ObjectId']
            elif kind == 'catalog': catalog.append({**catalog[0], 'ObjectId': 200})
            elif kind == 'date': rows[0]['date'] = True
            else: rows[0]['date'] = (h.NOW+timedelta(days=1)).date().isoformat()
            with self.assertRaises(ValueError): m.build(rows, catalog, acquisition, h.NOW.isoformat())

    def test_unregistered_country_conflicting_and_same_named_ports_are_inspectable(self):
        rows, catalog, acquisition = self.fixture()
        catalog[0]['portname'] = catalog[1]['portname'] = 'Same name'
        rows[0]['ISO3'] = 'OTH'
        rows.append({**rows[-1], 'portid': 'unregistered', 'ObjectId': 200})
        result = m.build(rows, catalog, acquisition, h.NOW.isoformat())
        self.assertEqual(len(result['ports']), 5); self.assertEqual(result['ports'][-1]['catalog_status'], 'unregistered')
        self.assertFalse(result['ports'][0]['country_identity_consistent'])
        self.assertIn('Same name', result['ports'][0]['names']); self.assertIn('Same name', result['ports'][1]['names'])
        self.assertEqual(result['covered_port_cohort']['legs']['import']['matched_port_ids'], ['p1', 'p2'])

    def test_boolean_negative_fractional_and_overflow_values_never_form_complete_windows(self):
        for value in (True, -1, 0.25, m.MAX_EXACT+1, None):
            rows, catalog, acquisition = self.fixture(); rows[41]['import'] = value
            self.assertIsNone(m.build(rows, catalog, acquisition, h.NOW.isoformat())['ports'][0]['legs']['import']['current_7d']['mean_metric_tons_per_day'])
        rows, catalog, acquisition = self.fixture()
        for row in rows: row['import'] = m.MAX_EXACT
        leg = m.build(rows, catalog, acquisition, h.NOW.isoformat())['ports'][0]['legs']['import']['current_7d']
        self.assertEqual(leg['status'], 'outside_exact_json_range'); self.assertIsNone(leg['sum_metric_tons'])

    def test_complete_native_large_population_uses_post_and_never_top_k(self):
        memory, rows, calls, opener = h.fixture(); template = deepcopy(rows[:42]); rows.clear(); opener.catalog.clear()
        for p in range(2101):
            opener.catalog.append({'ObjectId': p+1, 'portid': 'port'+str(p), 'portname': 'Fixture '+str(p), 'country': 'Fixture country', 'ISO3': 'FIX'})
            for item in template: rows.append({**item, 'ObjectId': len(rows)+1, 'portid': 'port'+str(p), 'portname': 'Fixture '+str(p)})
        methods = []
        def observed(req, timeout=None): methods.append(req.get_method()); return opener(req, timeout=timeout)
        self.assertTrue(h.Tests().execute(memory, observed)['published'])
        packet = h.store.decode(memory.data[h.store.HEAD]); review = packet['measurement_review']
        self.assertEqual((review['catalog_ports'], review['port_rows'], review['observation_rows']), (2101, 2101, 88242))
        self.assertEqual(review['covered_port_cohort']['legs']['import']['matched_ports'], 2101)
        self.assertGreater(methods.count('POST'), 80)
        self.assertTrue(all(q['returned_rows'] == q['declared_count'] == q['enumerated_ids'] for q in packet['acquisition_review']['queries']))
        with contextlib.redirect_stdout(StringIO()): replay = h.store.replay(h.native, memory, h.native.BUCKET, packet)
        self.assertEqual(replay['calendar_port_rows'], 2101)

    def test_incomplete_counts_ids_features_flags_or_metadata_refuse_publication(self):
        for case in ('count', 'ids', 'features', 'flag', 'schema', 'foreign_id'):
            memory, _, _, opener = h.fixture(); previous = memory.data[h.store.HEAD]
            def broken(req, timeout=None):
                response = opener(req, timeout=timeout); packet = h.store.strict(response.read())
                params = dict(urllib.parse.parse_qsl(req.data.decode() if req.data else urllib.parse.urlsplit(req.full_url).query))
                if case == 'count' and 'count' in packet: packet['count'] += 1
                elif case == 'ids' and 'objectIds' in packet: packet['objectIds'].append(packet['objectIds'][0])
                elif case == 'features' and 'objectIds' in params: packet['features'].pop()
                elif case == 'flag' and 'objectIds' in params: packet['exceededTransferLimit'] = 0
                elif case == 'schema' and 'fields' in packet: packet['objectIdField'] = 'other'
                elif case == 'foreign_id' and 'objectIds' in params: packet['features'][0]['attributes']['ObjectId'] = 999999
                return h.store.Response(h.store.encode(packet))
            with self.assertRaises(ValueError): h.Tests().execute(memory, broken)
            self.assertEqual(memory.data[h.store.HEAD], previous)

    def test_source_origin_changed_schema_or_insufficient_budget_cannot_fall_back(self):
        with self.assertRaises(ValueError): m.acquire(lambda *a: None, m.CATALOG+'/query', h.NOW.isoformat(), lambda: 100)
        calls = []
        with self.assertRaises(ValueError): m.acquire(lambda *a: calls.append(a), m.DAILY+'/query', h.NOW.isoformat(), lambda: 0)
        self.assertEqual(calls, [])
        schemas = json.loads((h.ROOT/'docs/audit/2026-09-27/portwatch-provider-schema-review.json').read_bytes())['layers']
        original = schemas['Daily_Ports_Data']
        for case in ('type', 'missing', 'duplicate'):
            value = deepcopy(original)
            if case == 'type': next(f for f in value['fields'] if f['name'] == 'import')['type'] = 'esriFieldTypeString'
            elif case == 'missing': value['fields'] = [f for f in value['fields'] if f['name'] != 'export']
            else: value['fields'].append(value['fields'][0])
            with self.assertRaises(ValueError): m.schema(value, 'Daily_Ports_Data', True)


if __name__ == '__main__': unittest.main()
