"""Exercise actual immutable TIC storage and independent integer arithmetic."""
import ast
from copy import deepcopy
from datetime import datetime, timedelta
import hashlib
import importlib.util
from pathlib import Path
import sys
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT/'aws/ops/checks'))
import calls_tic_lineage as candidate

spec = importlib.util.spec_from_file_location('tic_original_fixture', ROOT/'aws/lambdas/justhodl-capital-inflows/tests/test_originals.py')
fixture = importlib.util.module_from_spec(spec); spec.loader.exec_module(fixture)
AT = fixture.AT


def snapshot(edit=None, generated=AT, decorate=False):
    client, inputs, document = fixture.fixture()
    if edit:
        edit(document)
        inputs['originals']['bulk'] = fixture.archive(client, document)
    if decorate:
        for item in inputs['originals'].values():
            item['unselected'] = 'DESCRIPTOR_CANARY'; item['evidence']['unselected'] = 'EVIDENCE_CANARY'
        inputs['originals']['unreviewed'] = {'secret': 'EXTRA_ORIGINAL_CANARY', 'key': 'audit-private/do-not-read'}
    with patch.object(candidate.store, 'collect', return_value=(inputs['originals'], {}, fixture.VINTAGE)), \
         patch.object(candidate.store, 'preserve', return_value=inputs['legacy']), \
         patch.object(candidate.store, 'now', return_value=generated):
        result = candidate.store.run(client, 'b', 'fixture-no-network')
    assert result['published']
    read = candidate.store.raw_reader(client, 'b')
    return client.objects[candidate.store.model.CURRENT], read, document, client


def inspect(raw, read, at=AT): return candidate.inspect(raw, read, at)


def independent_inputs(raw, read):
    packet = candidate.strict(raw)
    manifest = candidate.strict(read(packet['replay']['manifest_key']))
    retained = candidate.ImmutableReader(read)
    candidate.store.replay(manifest, retained)
    return packet, {key: candidate.strict(value) for key, value in retained.cache.items() if key.startswith('data/tic-research/histories/')}


class TicOriginalQualification(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.raw, read, cls.document, cls.client = snapshot()
        cls.read = staticmethod(read); cls.result = inspect(cls.raw, read)
        cls.packet, cls.histories = independent_inputs(cls.raw, read)

    def test_complete_native_histories_windows_coordinates_zero_and_sign(self):
        out = self.result; proof = out['independent_arithmetic']
        self.assertEqual(proof['native_rows'], 324)
        self.assertEqual(proof['windows'], 363)
        self.assertEqual(proof['incomplete_windows'], 99)
        self.assertEqual(proof['reconciliations'], 72)
        self.assertEqual(proof['unreconciled'], 0)
        self.assertEqual(out['coverage']['complete_history_shards'], 20)
        self.assertEqual(out['coverage']['selected_original_occurrences'], 120)
        self.assertEqual(out['reported_headline']['net_cross_border_lt_12mo_b'], 18)
        self.assertEqual(out['reported_headline']['short_term_treasury_12mo_b'], 0)
        self.assertEqual(out['reported_holder_splits']['official']['sum_12m'], 12)
        self.assertTrue(out['current_use']['eligible'])
        for key in ('calls_eligible', 'sizing_eligible', 'execution_eligible', 'publication_eligible'):
            self.assertIs(out[key], False)
        for window in out['windows'].values():
            self.assertEqual(len(window['months']), 12)
            for period, identity in window['original_observations'].items():
                point = out['observations'][identity]
                original = self.document['series'][point['original_series_index']]['observations'][point['original_row']]
                self.assertEqual(original, [period, point['reported_native_value']])
                self.assertEqual(point['unit'], 'usd_million')
        self.assertIsNone(out['independent_evidence_count'])
        self.assertFalse(proof['historical_point_in_time_verified'])
        self.assertFalse(proof['fred_parity_independently_verified'])

    def test_independent_checker_rejects_corrupted_derived_amounts_and_calendars(self):
        sid = 'FORLTTOTALNET99996'
        changes = [lambda p: p['headline'].update(net_cross_border_lt_12mo_b=-18),
                   lambda p: p['headline'].update(latest_month_b=4000),
                   lambda p: p['headline'].update(yoy_change_12mo_b=48),
                   lambda p: p['headline'].update(run_rate_3mo_annualized_b=12),
                   lambda p: p['headline'].update(short_term_treasury_12mo_b=False),
                   lambda p: p['measurements'][sid].update(series_index=True),
                   lambda p: p['measurements'][sid].update(id='ANOTHER_SERIES'),
                   lambda p: p['measurements'][sid].update(value_decimal='4001'),
                   lambda p: p['measurements'][sid].update(unit='usd_bn'),
                   lambda p: p['measurements'].update(extra=deepcopy(p['measurements'][sid])),
                   lambda p: p.update(units='usd_million'),
                   lambda p: p['exact_headline']['prior_nonoverlapping_twelve_months']['months'].pop(),
                   lambda p: p['exact_headline']['latest_three_months']['source_rows'].__setitem__(0, False),
                   lambda p: p['holder_splits']['lt_total']['official'].update(sum_12m=12000),
                   lambda p: p['holder_splits']['lt_total'].update(rolling_twelve_months_reconciled=1),
                   lambda p: p['history_12mo_rolling_b'].pop(),
                   lambda p: p['by_asset_class']['equities'].update(series_id='TREASURIES')]
        for i, change in enumerate(changes):
            changed = deepcopy(self.packet); change(changed)
            with self.subTest(mutation=i), self.assertRaises(ValueError):
                candidate.arithmetic.verify(changed, self.document, self.histories)
        tree = ast.parse(Path(candidate.arithmetic.__file__).read_text(encoding='utf-8'))
        imports = {node.module if isinstance(node, ast.ImportFrom) else item.name for node in ast.walk(tree)
                   if isinstance(node, (ast.Import, ast.ImportFrom)) for item in node.names}
        self.assertEqual(imports, {'datetime', 'fractions', 'math', 're'})

    def test_independent_checker_checks_old_rows_and_all_decompositions(self):
        recon_key = self.packet['reconciliation']['history']['key']
        rolling_key = self.packet['rolling_history']['key']
        native_key = self.packet['measurements']['FORLTTOTALNET99996']['history']['key']
        changes = [lambda h: h[native_key]['rows'][0].update(value_decimal='99'),
                   lambda h: h[native_key].update(unit='usd_bn'),
                   lambda h: h[native_key].update(series_id='OTHER'),
                   lambda h: h[recon_key]['rows'][0]['asset_classes'].update(rounding_bound_usd_million_decimal='3'),
                   lambda h: h[recon_key]['rows'][0]['official_private'].update(status='unreconciled'),
                   lambda h: h[recon_key]['rows'][0]['official_private']['components']['private'].update(row_index=False),
                   lambda h: h[rolling_key]['rows'][0]['totals']['total'].update(usd_bn_decimal='0'),
                   lambda h: h[rolling_key]['rows'][-1]['totals']['private']['months'].pop(),
                   lambda h: h[rolling_key]['rows'][13].update(net_cross_border_lt_12mo_b=-18),
                   lambda h: h[rolling_key]['field_units'].update({'rows.*.rolling_12mo_b': 'usd_million'})]
        for i, change in enumerate(changes):
            changed = deepcopy(self.histories); change(changed)
            with self.subTest(mutation=i), self.assertRaises(ValueError):
                candidate.arithmetic.verify(self.packet, self.document, changed)

    def test_missing_month_and_missing_value_cannot_be_zero_or_previous_period(self):
        for missing in ('absent', '.'):
            def edit(document):
                row = next(v for v in document['series'] if v['source_id'] == 'for_lt_total_net_99996')
                if missing == 'absent': del row['observations'][4]
                else: row['observations'][4][1] = missing
            raw, read, _, _ = snapshot(edit); out = inspect(raw, read)
            self.assertFalse(out['current_use']['eligible'])
            self.assertIsNone(out['current_research'])
            self.assertIsNone(out['reported_headline']['foreign_net_into_us_lt_12mo_b'])
            self.assertEqual(out['windows']['total']['missing_months'], ['2026-03-01'])
            point = out['windows']['total']['original_observations']['2026-03-01']
            if missing == 'absent': self.assertIsNone(point)
            else: self.assertEqual(out['observations'][point]['reported_native_value'], '.')

    def test_historical_rounding_and_missing_holder_are_not_silently_reconciled(self):
        for value, status in (('1001', 'within_reporting_rounding'), ('1002', 'unreconciled'), ('.', 'incomplete')):
            def edit(document):
                row = next(v for v in document['series'] if v['source_id'] == 'for_lt_total_net_99990')
                row['observations'][3][1] = value
            raw, read, doc, _ = snapshot(edit); out = inspect(raw, read)
            packet, histories = independent_inputs(raw, read)
            rows = histories[packet['reconciliation']['history']['key']]['rows']
            self.assertEqual(rows[-4]['official_private']['status'], status)
            self.assertEqual(out['reported_holder_splits']['official']['latest'], 1)
            if status in ('unreconciled', 'incomplete'):
                self.assertIsNone(out['reported_holder_splits']['official']['sum_12m'])
            self.assertFalse(out['current_use']['eligible'])  # FRED/native disagreement also remains visible.
            candidate.arithmetic.verify(packet, doc, histories)

    def test_nonoverlapping_annual_difference_and_signed_flow(self):
        def edit(document):
            for item in document['series']:
                if item['source_id'] == 'for_lt_total_net_99996':
                    for i, row in enumerate(item['observations']): row[1] = '3000' if i >= 12 else '4000'
                if item['source_id'] == 'us_lt_total_net_99996':
                    for row in item['observations']: row[1] = '5000'
        raw, read, _, _ = snapshot(edit); out = inspect(raw, read)
        self.assertEqual(out['reported_headline']['yoy_change_12mo_b'], 12)
        self.assertEqual(out['reported_headline']['net_cross_border_lt_12mo_b'], -12)
        self.assertFalse(set(out['windows']['total']['months']) & set(out['windows']['total_prior_nonoverlapping_12m']['months']))
        self.assertFalse(out['current_use']['eligible'])

    def test_expiry_boundary_and_new_wrapper_do_not_renew_originals(self):
        exact = (datetime.fromisoformat(AT)+timedelta(hours=26)).isoformat()
        self.assertTrue(inspect(self.raw, self.read, exact)['current_use']['eligible'])
        after = (datetime.fromisoformat(exact)+timedelta(seconds=1)).isoformat()
        expired = inspect(self.raw, self.read, after)
        self.assertFalse(expired['current_use']['eligible'])
        self.assertIsNone(expired['current_research'])
        for key in ('observations', 'windows', 'reported_headline', 'reported_holder_splits'):
            self.assertEqual(expired[key], self.result[key])
        self.assertIn('native_packet:publication_age', expired['current_use']['issues'])
        generated = '2026-09-20T18:00:00+00:00'
        raw, read, _, _ = snapshot(generated=generated); out = inspect(raw, read, generated)
        self.assertFalse(out['current_use']['eligible'])
        self.assertNotIn('native_packet:publication_age', out['current_use']['issues'])
        self.assertTrue(any(issue.endswith(':acquisition_age') for issue in out['current_use']['issues']))

    def test_period_ids_survive_reordering_while_occurrence_ids_change(self):
        def edit(document):
            document['series'].reverse()
            for item in document['series']: item['observations'].reverse()
        raw, read, _, _ = snapshot(edit); out = inspect(raw, read)
        self.assertEqual({v['period_id'] for v in out['observations'].values()}, {v['period_id'] for v in self.result['observations'].values()})
        self.assertFalse(set(out['observations']) & set(self.result['observations']))
        self.assertEqual(out['reported_headline'], self.result['reported_headline'])

    def test_full_packet_and_each_immutable_kind_are_hash_bound(self):
        for edit in (lambda p: p['headline'].update(latest_month_b=9), lambda p: p.update(private_canary='DO_NOT_COPY'),
                     lambda p: p['replay'].update(manifest_key='data/capital-inflows.json')):
            changed = deepcopy(self.packet); edit(changed)
            with self.assertRaises(ValueError): inspect(candidate.encoded(changed), self.read)
        for prefix in ('runs', 'compilers', 'inputs', 'outputs', 'histories'):
            key = next(v['key'] for v in self.result['retained_artifact_inventory'] if v['key'].startswith('data/tic-research/'+prefix+'/'))
            with self.subTest(kind=prefix), self.assertRaisesRegex(ValueError, 'hash differs'):
                inspect(self.raw, lambda k: b'{}' if k == key else self.read(k))
        key = self.result['original_sources']['bulk']['evidence']['key']
        with self.assertRaisesRegex(ValueError, 'hash differs'):
            inspect(self.raw, lambda k: b'{}' if k == key else self.read(k))
        with self.assertRaisesRegex(ValueError, 'Future source packet'): inspect(self.raw, self.read, '2026-09-19T14:59:59Z')

    def test_unknown_metadata_never_enters_projection_or_transports_unknown_source(self):
        raw, read, _, _ = snapshot(decorate=True); called = []
        def guarded(key): called.append(key); return read(key)
        out = inspect(raw, guarded); encoded = candidate.encoded(out)
        manifest = candidate.strict(read(candidate.strict(raw)['replay']['manifest_key']))
        self.assertIn(b'EXTRA_ORIGINAL_CANARY', read(manifest['input']['key']))
        for canary in (b'DESCRIPTOR_CANARY', b'EVIDENCE_CANARY', b'EXTRA_ORIGINAL_CANARY'):
            self.assertNotIn(canary, encoded)
        self.assertNotIn('audit-private/do-not-read', called)
        self.assertEqual(len(out['original_sources']), 21)

    def test_immutable_reader_rejects_paths_before_io_and_bounds_whole_artifacts(self):
        calls = []
        def read(key): calls.append(key); return self.read(key)
        out = inspect(self.raw, read)
        self.assertEqual(len(calls), len(set(calls)))
        self.assertEqual(len(calls), out['coverage']['retained_artifacts'])
        guard = candidate.ImmutableReader(read); before = len(calls)
        for key in ('data/capital-inflows.json', 'data/prospective-outcomes.json', 'audit-private/original',
                    'data/tic-research/current.json', 'data/tic-research/runs/../output.json', None):
            with self.assertRaises(ValueError): guard(key)
        self.assertEqual(len(calls), before)
        with patch.object(candidate, 'MAX_ARTIFACTS', 1), self.assertRaisesRegex(ValueError, 'count exceeds bound'):
            inspect(self.raw, read)
        with patch.object(candidate, 'MAX_TOTAL', 1), self.assertRaisesRegex(ValueError, 'bytes exceed bound'):
            inspect(self.raw, read)
        body = b'whole'; key = 'data/tic-research/runs/'+hashlib.sha256(body).hexdigest()+'.json'
        for data in (b'', 'text', b'bad'):
            with self.assertRaises(ValueError): candidate.ImmutableReader(lambda _: data)(key)
        with patch.object(candidate.store, 'MAX_BYTES', 3), self.assertRaises(ValueError):
            candidate.ImmutableReader(lambda _: body)(key)

    def test_independent_native_identity_precision_and_calendar_validation(self):
        edits = [lambda d: d['series'].append(deepcopy(d['series'][0])),
                 lambda d: d['series'][0]['metadata'].update(units='Billions of Dollars'),
                 lambda d: d['series'][0]['metadata'].update(frequency='Q'),
                 lambda d: d['series'][0]['metadata']['additional'].update(status='I'),
                 lambda d: d['series'][0]['observations'].append(deepcopy(d['series'][0]['observations'][0])),
                 lambda d: d['series'][0]['observations'][0].__setitem__(0, '2026-07-02'),
                 lambda d: d['series'][0]['observations'][0].__setitem__(0, '2027-07-01'),
                 lambda d: d['series'][0]['observations'][0].__setitem__(1, '0.25')]
        for i, edit in enumerate(edits):
            doc = deepcopy(self.document); edit(doc)
            with self.subTest(mutation=i), self.assertRaises(ValueError):
                candidate.arithmetic.verify(self.packet, doc, self.histories)
        for value in (True, False, 'NaN', 'Infinity', '1/1', '1e999999999', '1e-999999999', '1e16', 1.5):
            with self.subTest(value=value), self.assertRaises(ValueError): candidate.arithmetic.amount(value)
        for value in (None, '.', '', 'NA'): self.assertIsNone(candidate.arithmetic.amount(value))
        self.assertEqual(candidate.arithmetic.amount('-0'), 0)
        self.assertEqual(candidate.arithmetic.amount('1e3'), 1000)
        for raw in (b'{"a":1,"a":2}', b'{"a":NaN}', b'{"a":Infinity}'):
            with self.assertRaises(ValueError): candidate.strict(raw)


if __name__ == '__main__': unittest.main(verbosity=2)
