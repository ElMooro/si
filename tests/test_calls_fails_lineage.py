"""Complete synthetic FR2004 originals through the unpublished Calls boundary."""
from copy import deepcopy
from datetime import timedelta
from pathlib import Path
import ast
import hashlib
import sys
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT/'aws/ops/checks'), str(ROOT/'aws/lambdas/justhodl-settlement-fails/tests')]
import calls_fails_lineage as model
import test_research as fixture


def snapshot(edit=None, at=None, decorate=None):
    client, definitions, raws, inputs = fixture.fixtures()
    if edit:
        rows = model.store.native.strict_json(raws['observations']); edit(rows['pd']['timeseries'])
        raw = model.encoded(rows)
        inputs['sources']['observations']['evidence'] = fixture.capture(client, 'fixture', 'fr2004',
            model.store.native.DATA_URL, raw, fixture.datetime.fromisoformat(fixture.AT))
    if decorate: decorate(inputs)
    # The synthetic PDF identities are explicit test fixtures. No production
    # definitions or archived source are changed or executed by these tests.
    with patch.dict(model.store.native.DEFINITIONS, definitions, clear=True), \
         patch.object(model.store, 'now', return_value=at or fixture.AT), \
         patch.object(model.store, 'acquire', side_effect=lambda _c, _b, key: inputs['sources'][key]):
        model.store.run(client, 'fixture')
    return client.objects[model.store.model.CURRENT], client, definitions


def inspect(raw, client, definitions, at=fixture.AT):
    with patch.dict(model.store.native.DEFINITIONS, definitions, clear=True):
        return model.inspect(raw, model.store.raw_reader(client, 'fixture'), at)


class CallsFailsLineage(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.raw, cls.client, cls.definitions = snapshot()
        cls.result = inspect(cls.raw, cls.client, cls.definitions)

    def test_complete_original_population_and_all_scope_history(self):
        result = self.result
        self.assertEqual(result['coverage']['original_series'], 12)
        self.assertEqual(result['coverage']['original_rows'], 1272)
        self.assertEqual(result['coverage']['scopes'], 8)
        self.assertEqual(result['coverage']['scope_history_rows'], 848)
        self.assertEqual(result['independent_arithmetic']['scope_history_rows_checked'], 954)
        self.assertEqual(result['independent_arithmetic']['integer_sums_checked'], 2862)
        self.assertTrue(all(scope['current_use']['eligible'] for scope in result['scopes'].values()))
        self.assertTrue(all(result[field] is False for field in
            ('calls_eligible', 'sizing_eligible', 'execution_eligible', 'publication_eligible')))
        self.assertFalse(result['independent_arithmetic']['statistics_independently_verified'])

    def test_treasury_overlap_and_exact_original_coordinates(self):
        result = self.result
        self.assertEqual(result['overlap']['shared_series'], ['PDFTD-USTET', 'PDFTR-USTET'])
        self.assertEqual(result['overlap']['additional_treasury_series'], ['PDFTD-UST', 'PDFTR-UST'])
        self.assertIsNone(result['overlap']['independent_evidence_count'])
        for scope in result['scopes'].values():
            for calculation in scope['history']:
                self.assertEqual(calculation['calculation_id'], 'fr2004-calculation-'+model.digest(
                    {k: v for k, v in calculation.items() if k != 'calculation_id'}))
                for series, identity in calculation['original_observations'].items():
                    row = result['observations'][identity]
                    self.assertEqual(row['series_id'], series)
                    self.assertEqual(row['observation_date'], calculation['date'])
        self.assertEqual(result['scopes']['ust_ex_tips']['reported']['gross_bn'], 201)
        self.assertEqual(result['scopes']['treasury_incl_tips']['reported']['gross_bn'], 406)

    def test_period_identity_survives_reordering_and_revision_but_occurrence_does_not(self):
        raw, client, defs = snapshot(lambda rows: (rows.reverse(), rows[0].update(value='42')))
        revised = inspect(raw, client, defs)
        old = {r['period_id']: (key, r) for key, r in self.result['observations'].items()}
        new = {r['period_id']: (key, r) for key, r in revised['observations'].items()}
        self.assertEqual(set(old), set(new))
        self.assertTrue(all(old[key][0] != new[key][0] for key in old))
        self.assertEqual(sum(old[key][1]['reported_native_value'] != new[key][1]['reported_native_value'] for key in old), 1)

    def test_missing_suppressed_and_zero_stay_distinct_in_whole_history(self):
        def edit(rows):
            rows[0]['value'] = '*'; rows[106]['value'] = '0'; rows[212]['value'] = ''
            rows.pop(318)  # Missing TIPS receive observation at the newest date.
        raw, client, defs = snapshot(edit)
        result = inspect(raw, client, defs)
        headline = result['scopes']['ust_ex_tips']
        self.assertIsNone(headline['reported']['ftd_bn']); self.assertEqual(headline['reported']['ftr_bn'], 0)
        self.assertIsNone(headline['reported']['gross_bn']); self.assertFalse(headline['current_use']['eligible'])
        self.assertEqual(headline['latest_calculation']['date'], '2026-09-09')
        tips = result['scopes']['tips']['latest_calculation']
        self.assertIsNone(tips['original_observations']['PDFTR-UST'])
        self.assertEqual(result['coverage']['original_rows'], 1271)
        self.assertGreater(result['independent_arithmetic']['incomplete_scope_rows_preserved'], 0)

    def test_expiry_preserves_every_source_row_and_historical_calculation(self):
        later = (model.store.native.clock(fixture.AT)+timedelta(hours=37)).isoformat()
        result = inspect(self.raw, self.client, self.definitions, later)
        self.assertEqual(result['observations'], self.result['observations'])
        for key, scope in result['scopes'].items():
            self.assertEqual(scope['history'], self.result['scopes'][key]['history'])
            self.assertIsNone(scope['current_research']); self.assertFalse(scope['current_use']['eligible'])
            self.assertIn('native_packet:publication_age', scope['current_use']['issues'])
            self.assertIn('acquisition_older_than_36h', scope['current_use']['issues'])

    def test_new_packet_clock_cannot_refresh_old_originals(self):
        raw, client, defs = snapshot(at='2026-09-20T11:00:00Z')
        result = inspect(raw, client, defs, '2026-09-21T00:00:00Z')
        for scope in result['scopes'].values():
            self.assertNotIn('native_packet:publication_age', scope['current_use']['issues'])
            self.assertIn('acquisition_older_than_36h', scope['current_use']['issues'])
            self.assertIsNone(scope['current_research'])

    def test_complete_packet_original_and_manifest_mutations_are_rejected(self):
        for kind in ('value', 'authority', 'extra', 'reference'):
            packet = model.store.native.strict_json(self.raw)
            if kind == 'value': packet['treasury']['gross_bn'] += 1
            elif kind == 'authority': packet['calls_eligible'] = True
            elif kind == 'extra': packet['unselected_private_canary'] = 'must-not-leak'
            else: packet['replay']['manifest_key'] = 'data/settlement-fails.json'
            with self.assertRaises(ValueError): inspect(model.encoded(packet), self.client, self.definitions)
        keys = (self.result['source_replay']['manifest_key'], self.result['original_sources']['observations']['evidence']['key'])
        for bad in keys:
            base = model.store.raw_reader(self.client, 'fixture')
            with patch.dict(model.store.native.DEFINITIONS, self.definitions, clear=True), \
                 self.assertRaisesRegex(ValueError, 'hash differs'):
                model.inspect(self.raw, lambda key: b'{}' if key == bad else base(key), fixture.AT)
        with self.assertRaisesRegex(ValueError, 'Future source packet'):
            inspect(self.raw, self.client, self.definitions, '2026-09-19T10:59:59Z')

    def test_independent_checker_catches_scope_conversion_and_coordinate_failures(self):
        packet = model.store.native.strict_json(self.raw)
        original = model.store.native.strict_json(model.store.raw_reader(self.client, 'fixture')(
            self.result['original_sources']['observations']['evidence']['key']))
        mutations = [lambda p: p['treasury']['history'][-1].update(gross_usd_mn=1),
            lambda p: p['headline']['exact_usd_bn'].update(gross='0.201'),
            lambda p: p['classes'][0]['history'][0]['components']['PDFTD-USTET'].update(row_index=7),
            lambda p: p['totals']['history'].pop(), lambda p: p['classes'][2].update(ftd_bn=True),
            lambda p: p['headline'].update(source_series=['PDFTD-UST', 'PDFTR-UST'])]
        for edit in mutations:
            changed = deepcopy(packet); edit(changed)
            with self.assertRaises(ValueError): model.arithmetic.verify(changed, original)
        tree = ast.parse(Path(model.arithmetic.__file__).read_text(encoding='utf-8'))
        imports = {node.module if isinstance(node, ast.ImportFrom) else item.name
            for node in ast.walk(tree) if isinstance(node, (ast.Import, ast.ImportFrom))
            for item in node.names}
        self.assertEqual(imports, {'datetime', 'fractions', 'math', 're'})

    def test_immutable_read_guard_rejects_before_io_caches_complete_bytes_and_bounds(self):
        calls = []; base = model.store.raw_reader(self.client, 'fixture')
        def read(key): calls.append(key); return base(key)
        with patch.dict(model.store.native.DEFINITIONS, self.definitions, clear=True):
            result = model.inspect(self.raw, read, fixture.AT)
        self.assertEqual(len(calls), len(set(calls)))
        self.assertEqual(len(calls), result['coverage']['retained_artifacts'])
        self.assertEqual(sum(r['bytes'] for r in result['retained_artifact_inventory']), result['coverage']['retained_uncompressed_bytes'])
        guard = model.ImmutableReader(read); count = len(calls)
        for key in ('data/settlement-fails.json', 'data/prospective-outcomes.json', 'audit-private/original',
                    'data/fails-research/migration.json', 'data/fails-research/runs/../current.json', None):
            with self.assertRaises(ValueError): guard(key)
        self.assertEqual(len(calls), count)
        with patch.object(model, 'MAX_TOTAL', 1), self.assertRaisesRegex(ValueError, 'bytes exceed bound'):
            inspect(self.raw, self.client, self.definitions)


if __name__ == '__main__': unittest.main(verbosity=2)
