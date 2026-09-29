"""Actual CISS storage/replay boundary plus an independent arithmetic oracle."""
import ast
from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
from datetime import datetime
import gzip
import hashlib
from pathlib import Path
import sys
import threading
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT/'aws/ops/checks'), str(ROOT/'aws/shared/tests')]
import calls_ciss_lineage as candidate
import test_ciss_source_model as fixture
import test_ciss_source_store as storage


def snapshot(edit=None, generated=fixture.NOW, decorate=False):
    client = storage.MemoryS3(); discoveries, histories = fixture.packet_inputs()
    if edit: edit(discoveries, histories)
    for entries in (discoveries, histories):
        for key, item in entries.items():
            url = storage.store.BASE+key.replace('.', '/', 1)+'?format=csvdata' if '.' in key else storage.store.BASE+key+'?format=csvdata&lastNObservations=1'
            item['request_url'] = url
            item['evidence'] = storage.capture(client, 'test', 'ecb', url, item['raw'], datetime.fromisoformat(item['acquired_at']))
            if decorate: item['private_canary'] = 'DESCRIPTOR_SECRET'; item['evidence']['unselected'] = 'EVIDENCE_SECRET'
    with patch.object(storage.store, 'collect', return_value=(generated, discoveries, histories, {})), patch.object(storage.store, 'datetime') as stamp:
        stamp.now.return_value = datetime.fromisoformat(generated)
        storage.store.run(client, 'test')
    def read(key):
        raw = client.objects[key]
        return gzip.decompress(raw) if key.endswith('.gz') else raw
    return client.objects[storage.store.CURRENT], read, discoveries, histories


def inspect(raw, read, at=fixture.NOW): return candidate.inspect(raw, read, at)


class CissOriginalQualification(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.raw, read, cls.discoveries, cls.histories = snapshot()
        cls.read = staticmethod(read); cls.result = inspect(cls.raw, read)

    def test_complete_population_calendar_coordinates_and_historical_mismatch_retained(self):
        out = self.result; proof = out['independent_arithmetic']
        self.assertEqual(out['coverage']['original_series'], 7); self.assertEqual(out['coverage']['original_rows'], 14)
        self.assertEqual(proof['calendar_comparisons_checked'], 28)
        self.assertEqual(proof['headline_panel'], {'matched': 1, 'mismatch': 1, 'unavailable': 0})
        self.assertTrue(out['current_headline_research_eligible'])
        self.assertTrue(all(out[key] is False for key in ('calls_eligible', 'sizing_eligible', 'execution_eligible', 'publication_eligible')))
        self.assertFalse(proof['statistics_independently_verified'])
        for key, view in out['series'].items():
            point = out['observations'][view['latest_occurrence']]
            self.assertEqual(point['series_id'], key); self.assertEqual(point['original_row'], 1)
            base = view['comparisons']['12m']; self.assertEqual(base['baseline_source_row'], 0)
            self.assertEqual(out['observations'][base['baseline_occurrence']]['observation_period'], '2025-09-15')

    def test_checker_catches_rehashed_coordinate_unit_value_and_panel_errors(self):
        packet = candidate.strict_json(self.raw)
        histories = {k: v['raw'] for k, v in self.histories.items()}; discoveries = {k: v['raw'] for k, v in self.discoveries.items()}
        changes = [lambda p: p['series'][0].update(source_row=0),
                   lambda p: p['series'][0].update(latest=True),
                   lambda p: p['series'][0].update(unit='percent'),
                   lambda p: p['series'][0]['comparisons']['12m'].update(value=1),
                   lambda p: p['series'][0]['comparisons']['12m'].update(baseline_source_row=1),
                   lambda p: p['series'][0]['comparisons']['12m'].update(baseline_source_row=False),
                   lambda p: p['series'][0]['chart_points'].pop(),
                   lambda p: p['series'][0]['annual_comparison'].update(value=.99),
                   lambda p: p['series'][0].update(chg_1y=.99),
                   lambda p: p['series'][0]['quality'].update(maximum_acquisition_age_seconds=999999),
                   lambda p: p['headline_reconciliation'].update(residual_decimal='1'),
                   lambda p: p['headline_reconciliation'].update(status='unavailable'),
                   lambda p: p.update(ea_composite=.9), lambda p: p['series'].pop()]
        for edit in changes:
            changed = deepcopy(packet); edit(changed)
            with self.assertRaises(ValueError): candidate.arithmetic.verify(changed, histories, discoveries)
        tree = ast.parse(Path(candidate.arithmetic.__file__).read_text(encoding='utf-8'))
        imports = {node.module if isinstance(node, ast.ImportFrom) else item.name for node in ast.walk(tree)
                   if isinstance(node, (ast.Import, ast.ImportFrom)) for item in node.names}
        self.assertEqual(imports, {'calendar', 'csv', 'datetime', 'fractions', 'io', 'math', 're'})

    def test_expiry_retains_every_historical_reference_and_rechecks_all_legs(self):
        boundary = inspect(self.raw, self.read, '2026-09-21T22:00:00Z')
        self.assertTrue(boundary['current_headline_research_eligible'])
        expired = inspect(self.raw, self.read, '2026-09-21T22:00:01Z')
        self.assertFalse(expired['current_headline_research_eligible'])
        self.assertEqual(expired['observations'], self.result['observations'])
        for key, view in expired['series'].items():
            self.assertEqual(view['reported'], self.result['series'][key]['reported'])
            self.assertEqual(view['comparisons'], self.result['series'][key]['comparisons'])
            self.assertIsNone(view['current_research'])
            self.assertIn('original:acquisition_age', view['current_use']['issues'])
        def older(_d, h): h[candidate.arithmetic.COMPONENTS[0]]['acquired_at'] = '2026-09-16T22:00:00Z'
        raw, read, _, _ = snapshot(older)
        out = inspect(raw, read, '2026-09-20T22:00:00Z')
        self.assertFalse(out['current_headline_research_eligible'])
        self.assertIn('headline:another_required_leg_unavailable', out['series'][candidate.arithmetic.HEAD]['current_use']['issues'])

    def test_new_packet_cannot_refresh_old_acquisition(self):
        raw, read, _, _ = snapshot(generated='2026-09-22T22:00:00+00:00')
        out = inspect(raw, read, '2026-09-22T22:00:00Z')
        self.assertFalse(out['current_headline_research_eligible'])
        for view in out['series'].values():
            self.assertNotIn('native_packet:publication_age', view['current_use']['issues'])
            self.assertIn('original:acquisition_age', view['current_use']['issues'])

    def test_period_ids_survive_reordered_response_but_occurrence_ids_change(self):
        def reorder(_d, histories):
            for item in histories.values():
                lines = item['raw'].splitlines(); item['raw'] = b'\n'.join([lines[0], *reversed(lines[1:])])+b'\n'
        raw, read, _, _ = snapshot(reorder); out = inspect(raw, read)
        self.assertEqual({v['period_id'] for v in out['observations'].values()}, {v['period_id'] for v in self.result['observations'].values()})
        self.assertFalse(set(out['observations']) & set(self.result['observations']))
        self.assertEqual(out['independent_arithmetic']['headline_panel'], self.result['independent_arithmetic']['headline_panel'])

    def test_missing_baseline_and_signed_zero_are_not_filled(self):
        def edit(_d, histories):
            for key, item in histories.items():
                latest = '-0.12' if key.endswith('.SS_CON.CON') else '.03'
                item['raw'] = fixture.csv_bytes(key, [('2025-09-15', '', 'M'), ('2026-08-14', '0', 'A'), ('2026-08-15', '', 'M'), ('2026-09-15', latest, 'A')])
        raw, read, _, _ = snapshot(edit); out = inspect(raw, read)
        for view in out['series'].values():
            for name in ('1m', '12m'):
                entry = view['comparisons'][name]; self.assertIsNone(entry['value'])
                self.assertIsNone(out['observations'][entry['baseline_occurrence']]['reported_decimal'])
        self.assertEqual(out['independent_arithmetic']['headline_panel']['unavailable'], 2)
        self.assertTrue(out['current_headline_research_eligible'])

    def test_monthly_leap_calendar_duplicate_and_future_rows_checked(self):
        key = 'CLIFS.M.AT._Z.4F.EC.CLIFS_CI.IDX'
        def edit(discoveries, histories):
            points = [('2025-02', '0', 'A'), ('2026-02', '0', 'A'), ('2026-07', '.1', 'A'), ('2026-07', '.1', 'A'), ('2026-10', '.2', 'A')]
            histories[key] = fixture.item(fixture.csv_bytes(key, points))
            discoveries['CLIFS'] = fixture.item(fixture.csv_bytes(key, [('2026-07', '.1', 'A')]))
        raw, read, _, _ = snapshot(edit); out = inspect(raw, read)
        self.assertEqual(out['coverage']['original_series'], 8); self.assertEqual(out['coverage']['original_rows'], 19)
        a = candidate.arithmetic
        self.assertEqual(a.target_day(a.date(2024, 2, 29), months=12).isoformat(), '2023-02-28')
        parsed, _ = a.parse(fixture.csv_bytes(key, [('2023-02', '.1', 'A'), ('2024-02', '.2', 'A')]), key)
        rows = parsed[key]['rows']; target, base = a.comparison(rows, rows[-1], 'M', months=12)
        self.assertEqual(target.isoformat(), '2023-02-28'); self.assertEqual(base['source_row'], 0)

    def test_missing_component_and_mismatched_sum_withhold_whole_headline(self):
        for value, status in (('', 'M'), ('-.11', 'A')):
            def edit(_d, histories):
                key = candidate.arithmetic.COMPONENTS[-1]
                histories[key]['raw'] = fixture.csv_bytes(key, [('2026-09-15', value, status)])
            raw, read, _, _ = snapshot(edit); out = inspect(raw, read)
            self.assertFalse(out['current_headline_research_eligible'])
            self.assertTrue(all(view['current_research'] is None for view in out['series'].values()))

    def test_complete_packet_and_original_mutations_fail_before_qualification(self):
        for edit in (lambda p: p.update(ea_composite=.8), lambda p: p.update(private_canary='DO_NOT_COPY'),
                     lambda p: p['replay'].update(manifest_key='data/ciss-stress.json')):
            packet = candidate.strict_json(self.raw); edit(packet)
            with self.assertRaises(ValueError): inspect(candidate.encoded(packet), self.read)
        with self.assertRaisesRegex(ValueError, 'Future source packet'): inspect(self.raw, self.read, '2026-09-18T21:59:59Z')
        key = next(iter(self.result['original_sources'].values()))['key']
        with self.assertRaisesRegex(ValueError, 'hash differs'): inspect(self.raw, lambda k: b'{}' if k == key else self.read(k))
        with self.assertRaisesRegex(ValueError, 'Duplicate JSON'): candidate.strict_json(b'{"a":1,"a":2}')

    def test_unknown_source_metadata_never_enters_candidate_projection(self):
        raw, read, _, _ = snapshot(decorate=True); self.assertIn(b'DESCRIPTOR_SECRET', read(candidate.strict_json(raw)['replay']['manifest_key']))
        out = candidate.encoded(inspect(raw, read))
        self.assertNotIn(b'DESCRIPTOR_SECRET', out); self.assertNotIn(b'EVIDENCE_SECRET', out)

    def test_reader_rejects_before_io_reads_each_whole_object_once_and_bounds(self):
        calls = []
        def read(key): calls.append(key); return self.read(key)
        out = inspect(self.raw, read)
        self.assertEqual(len(calls), len(set(calls))); self.assertEqual(len(calls), out['coverage']['retained_artifacts'])
        guard = candidate.ImmutableReader(read); before = len(calls)
        for key in ('data/ciss-stress.json', 'data/prospective-outcomes.json', 'audit-private/original', 'data/ciss-research/cache/a.json', None):
            with self.assertRaises(ValueError): guard(key)
        self.assertEqual(len(calls), before)
        with patch.object(candidate, 'MAX_TOTAL', 1), self.assertRaisesRegex(ValueError, 'bytes exceed bound'): inspect(self.raw, read)
        with patch.object(candidate, 'MAX_ARTIFACTS', 1), self.assertRaisesRegex(ValueError, 'count exceeds bound'): inspect(self.raw, read)
        raw = b'whole-original'; key = 'data/ciss-research/runs/'+hashlib.sha256(raw).hexdigest()+'.json'
        entered = threading.Event(); release = threading.Event(); actual = []
        def slow(k): actual.append(k); entered.set(); release.wait(5); return raw
        guard = candidate.ImmutableReader(slow)
        with ThreadPoolExecutor(max_workers=8) as pool:
            jobs = [pool.submit(guard, key) for _ in range(8)]; self.assertTrue(entered.wait(5)); release.set()
            self.assertTrue(all(job.result() == raw for job in jobs))
        self.assertEqual(actual, [key]); self.assertEqual(guard.bytes, len(raw))

    def test_independent_parser_rejects_malformed_csv_units_and_nonfinite_values(self):
        key = candidate.arithmetic.HEAD
        for raw in (fixture.csv_bytes(key, [('2026-09-15', 'NaN', 'A')]),
                    fixture.csv_bytes(key, [('2026-09-15', '.1', 'A')], unit='EUR'),
                    fixture.csv_bytes(key, [('2026-09-15', '.1', 'A'), ('2026-09-15', '.2', 'A')]),
                    fixture.csv_bytes(key, [('2026-09-15', '1e999999999', 'A')])):
            with self.assertRaises(ValueError): candidate.arithmetic.parse(raw, key)

    def test_zero_measurements_cannot_be_replaced_by_booleans(self):
        def edit(_d, histories):
            for key, item in histories.items(): item['raw'] = fixture.csv_bytes(key, [('2026-09-15', '0', 'A')])
        raw, _read, d, h = snapshot(edit)
        original = candidate.strict_json(raw)
        histories = {k: v['raw'] for k, v in h.items()}; discoveries = {k: v['raw'] for k, v in d.items()}
        self.assertEqual(candidate.arithmetic.verify(original, histories, discoveries)['latest_reconciliation'], 'matched')
        for edit in (lambda p: p['series'][0]['chart_points'][0].__setitem__(1, False),
                     lambda p: p['series'][0].update(duplicate_rows=False),
                     lambda p: p['series'][0].update(n_obs=True)):
            changed = deepcopy(original); edit(changed)
            with self.assertRaises(ValueError): candidate.arithmetic.verify(changed, histories, discoveries)


if __name__ == '__main__': unittest.main(verbosity=2)
