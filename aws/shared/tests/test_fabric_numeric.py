"""Invented whole producer/consumer regressions. No native or provider reads."""
from copy import deepcopy
import contextlib
import io
import json
from pathlib import Path
import socket
import sys
import types
import unittest
from unittest.mock import patch
sys.path[:0] = [str(Path(__file__).resolve().parents[1]), str(Path(__file__).resolve().parent)]
from ciss_vintage_test_support import load
from test_compound_numeric import Storage
from test_holdings_derived_boundary import Storage as ConsumerStorage
from fabric_numeric import number, stance, weight, symbol, collection, CONTRACT


class Numeric(unittest.TestCase):
    def test_zero_win_is_not_fifty(self):
        values = [weight('trend-reversal', {'board': [{'engine': 'trend-reversal', 'win_pct': win, 'n': 100}]}, {}, 'UNKNOWN')['value'] for win in (0, 50, 100)]
        self.assertEqual(values, [.2, .6, 1.5])

    def test_invalid_learning_record_cannot_get_maximum_or_fall_back(self):
        lb = {'board': [{'engine': 'trend-reversal', 'n': 100, 'win_pct': 100}]}
        for bad in (None, True, '50', float('nan'), float('inf'), -float('inf'), 51, -51):
            learned = {'by_engine': {'trend-reversal': {'n': 100, 'win': 100, 'lift': bad}}}
            self.assertIsNone(weight('trend-reversal', lb, learned, 'UNKNOWN')['value'])

    def test_exact_weight_identity_and_duplicates_are_order_independent(self):
        row = {'n': 100, 'win': 80, 'lift': 30}
        self.assertEqual(weight('trend-reversal', {}, {'by_engine': {'trend': row}}, 'UNKNOWN')['value'], .6)
        for table in ({'trend-reversal': row, 'justhodl-trend-reversal': row}, {'justhodl-trend-reversal': row, 'trend-reversal': row}):
            self.assertIsNone(weight('trend-reversal', {}, {'by_engine': table}, 'UNKNOWN')['value'])
        for board in ([{'engine': 'trend-reversal', 'win_pct': 0, 'n': 100}, {'engine': 'TREND-REVERSAL', 'win_pct': 100, 'n': 100}],):
            for rows in (board, board[::-1]):
                self.assertIsNone(weight('trend-reversal', {'board': rows}, {}, 'UNKNOWN')['value'])

    def test_regime_scope_sample_and_lift_consistency(self):
        learned = {'by_engine_regime': {'trend-reversal|RISK_ON': {'win': 80, 'lift': 30, 'n': 8}}, 'by_engine': {'trend-reversal': {'win': 50, 'lift': 0, 'n': 15}}}
        self.assertEqual(weight('trend-reversal', {}, learned, 'RISK_ON')['value'], 1.2)
        self.assertEqual(weight('trend-reversal', {}, learned, 'UNKNOWN')['value'], .6)
        for field, value in (('n', 7), ('n', True), ('n', 8.5), ('win', 60), ('lift', None)):
            p = deepcopy(learned); p['by_engine_regime']['trend-reversal|RISK_ON'][field] = value
            self.assertIsNone(weight('trend-reversal', {}, p, 'RISK_ON')['value'])

    def test_missing_direction_never_becomes_up(self):
        for direction in (None, '', 'UNKNOWN', True, [], 'TOP'):
            self.assertIsNone(stance('trend-reversal', {'reversal_score': 30, 'direction': direction}))
        self.assertEqual(stance('trend-reversal', {'reversal_score': 30, 'direction': 'TOP_FORMING'})[2], 'DOWN')

    def test_typed_units_and_field_presence(self):
        self.assertIsNone(stance('squeeze-fuel', {'days_to_cover': 100}))
        self.assertIsNone(stance('magic-formula', {'rank': None, 'magic_rank': 1}))
        self.assertIsNone(stance('opportunities', {'go_score': None, 'score': 100}))
        self.assertIsNone(stance('insider-clusters', {'insiders': True, 'n_insiders': 9}))
        for value in (True, '', '50', [], None, float('nan'), 10**500):
            self.assertIsNone(number(value, 0, 100))
        self.assertIsNone(stance('magic-formula', {'rank': 0}))
        self.assertIsNone(stance('magic-formula', {'rank': 1.5}))
        self.assertIsNone(stance('congress-direct', {'type': 'unknown'}))
        self.assertIsNone(stance('congress-direct', {'type': 'not a sale'}))
        self.assertEqual(stance('congress-direct', {'type': 'Sale (Partial)'})[2], 'DOWN')

    def test_identity_and_collection_presence(self):
        self.assertIsNone(symbol({'ticker': 'A', 'symbol': 'B'}))
        self.assertEqual(symbol({'ticker': ' a ', 'symbol': 'A'}), 'A')
        for canonical in (None, [], {}, True):
            self.assertEqual(collection({'rows': canonical, 'ranked': [{'ticker': 'LEGACY'}]}, ('rows', 'ranked'))[1], [])


class Whole(unittest.TestCase):
    def setUp(self):
        guard = patch.object(socket.socket, 'connect', side_effect=AssertionError('Invented inputs only'))
        guard.start(); self.addCleanup(guard.stop)

    def run_fabric(self, rows=None, extras=None, client=None):
        objects = {'data/trend-reversal.json': {'generated_at': '2001-01-01T00:00:00Z', 'rows': rows if rows is not None else [
            {'ticker': 'QAONLY', 'reversal_score': 30, 'direction': 'BOTTOM_FORMING'}]}, **(extras or {})}
        db = client or Storage(objects); m = load('justhodl-signal-fabric')
        with patch.object(m, 's3', db), contextlib.redirect_stdout(io.StringIO()):
            m.lambda_handler({}, None)
        return db

    def test_actual_reader_and_handler_cannot_turn_old_publication_into_current_vote(self):
        out = self.run_fabric().writes['data/signal-fabric.json']
        self.assertEqual(out['tickers'][0]['fabric_score'], .3)
        self.assertFalse(out['current_vote_eligible']); self.assertFalse(out['source_freshness_qualified'])
        source = out['source_context']['trend-reversal']
        self.assertEqual(source['packet']['generated_at'], '2001-01-01T00:00:00Z')
        self.assertFalse(source['current_vote_eligible']); self.assertFalse(out['tickers'][0]['engines'][0]['current_vote_eligible'])

    def test_missing_direction_and_invalid_ticker_do_not_inflate_counts(self):
        rows = [{'ticker': 'QAONLY', 'reversal_score': 30}, {'ticker': 'TOOLONGTICKER', 'reversal_score': 30, 'direction': 'BOTTOM_FORMING'}]
        out = self.run_fabric(rows).writes['data/signal-fabric.json']
        self.assertEqual(out['source_stats']['trend-reversal'], 0); self.assertEqual(out['n_tickers'], 0)
        self.assertEqual([r['record'] for r in out['source_context']['trend-reversal']['occurrences']], rows)

    def test_all_duplicate_occurrences_withheld_independent_of_arrival_order(self):
        rows = [{'ticker': 'QAONLY', 'reversal_score': 30, 'direction': d} for d in ('TOP_FORMING', 'BOTTOM_FORMING')]
        for supplied in (rows, rows[::-1], rows[:1] * 2):
            out = self.run_fabric(supplied).writes['data/signal-fabric.json']; row = out['tickers'][0]
            self.assertIsNone(row['net_direction']); self.assertIsNone(row['fabric_score']); self.assertEqual(row['n_engines'], 0)
            self.assertEqual(len(row['all_occurrences']), 2); self.assertEqual(out['source_stats']['trend-reversal'], 0)
            self.assertEqual([r['record'] for r in out['source_context']['trend-reversal']['occurrences']], supplied)

    def test_equal_opposing_contributions_are_neutral_and_conflict_visible(self):
        rows = [{'ticker': 'QAONLY', 'reversal_score': 36, 'direction': 'TOP_FORMING'}]
        out = self.run_fabric(rows, {'data/opportunities.json': {'rows': [{'ticker': 'QAONLY', 'score': 60}]}}).writes['data/signal-fabric.json']
        row = out['tickers'][0]; self.assertEqual(row['fabric_score'], 0); self.assertIsNone(row['net_direction'])
        self.assertEqual(row['agreement_pct'], 50); self.assertEqual(out['n_conflicts'], 1)

    def test_whole_reader_rejects_duplicate_json_nonfinite_and_truncation(self):
        for raw in (b'{"rows":[],"rows":[1]}', b'{"rows":[NaN]}'):
            out = self.run_fabric(extras={'data/trend-reversal.json': raw}).writes['data/signal-fabric.json']
            self.assertEqual(out['source_context']['trend-reversal']['status'], 'packet_unavailable')
            self.assertEqual(out['n_tickers'], 0)
        class Cut(Storage):
            def get_object(self, **kw):
                r = super().get_object(**kw); r['ContentLength'] += 1; self.last_body = r['Body']; return r
        db = self.run_fabric(client=Cut({})); self.assertTrue(db.last_body.closed)
        self.assertEqual(db.writes['data/signal-fabric.json']['n_tickers'], 0)

    def test_null_weight_cannot_crash_and_zero_win_reaches_output(self):
        out = self.run_fabric(extras={'data/learned-weights.json': {'by_engine': {'trend-reversal': {'n': 20, 'win': 50, 'lift': None}}}}).writes['data/signal-fabric.json']
        self.assertEqual(out['tickers'][0]['n_engines'], 0); self.assertIsNone(out['tickers'][0]['fabric_score'])
        out = self.run_fabric(extras={'data/engine-leaderboard.json': {'board': [{'engine': 'trend-reversal', 'n': 100, 'win_pct': 0}]}}).writes['data/signal-fabric.json']
        self.assertEqual(out['tickers'][0]['fabric_score'], .1)
        self.assertEqual(out['tickers'][0]['engines'][0]['weight_context']['records'][0]['record']['win_pct'], 0)

    def test_complete_population_over_every_legacy_cap_and_single_read(self):
        rows = [{'ticker': 'Q' + str(i), 'reversal_score': 30, 'direction': 'BOTTOM_FORMING'} for i in range(905)]
        db = self.run_fabric(rows)
        for key in ('data/signal-fabric.json', 'data/feature-bus.json'):
            self.assertEqual(db.writes[key]['n_tickers'], 905); self.assertEqual(len(db.writes[key]['tickers']), 905)
        self.assertEqual(db.reads.count('data/ai-rerating-radar.json'), 1)
        self.assertEqual(len(db.reads), len(set(db.reads)))
        stamps = {v['generated_at'] for v in db.writes.values()}; self.assertEqual(len(stamps), 1)

    def test_missing_prior_fields_or_matching_old_boundary_do_not_create_events(self):
        for prior in ({}, {'compound_research_boundary': 'compound-context-without-direction.v1'}, {'measurement_contract': CONTRACT, 'tickers': {'QAONLY': {'net_direction': 'DOWN'}}}):
            out = self.run_fabric(extras={'data/feature-bus.json': prior}).writes['data/fabric-events.json']
            self.assertEqual(out['events'], []); self.assertFalse(out['prior_calculation_comparable'])

    def test_peer_duplicates_do_not_take_last_group_and_self_is_excluded(self):
        rows = [{'ticker': s, 'reversal_score': 30, 'direction': 'BOTTOM_FORMING'} for s in ('QAA', 'QAB', 'QAC')]
        peers = [{'symbol': s, 'composite': 0, 'peer_group': 'invented'} for s in ('QAA', 'QAB', 'QAC')]
        good = self.run_fabric(rows, {'data/ai-rerating-radar.json': {'all_ranked': peers}}).writes['data/signal-fabric.json']
        for row in good['tickers']:
            self.assertEqual(row['peer_fabric_score'], .3)
            self.assertNotIn(row['ticker'], [r['ticker'] for r in row['peer_calculation']['other_members']])
        for p in (peers + [peers[0]], [peers[0]] + peers):
            out = self.run_fabric(rows, {'data/ai-rerating-radar.json': {'all_ranked': p}}).writes['data/signal-fabric.json']
            self.assertTrue(all('peer_fabric_score' not in r for r in out['tickers']))

    def test_output_budget_and_nonfinite_context_fail_before_any_write(self):
        m = load('justhodl-signal-fabric'); db = Storage({})
        with patch.object(m, 's3', db), patch.object(m, 'MAX_BYTES', 500):
            with self.assertRaises(ValueError): m.lambda_handler({}, None)
        self.assertEqual(db.writes, {})
        # A monkeypatched malformed parser result cannot pass publication either.
        with patch.object(m, 's3', db), patch.object(m, 'rd', return_value={'x': float('nan')}):
            with self.assertRaises(ValueError): m.lambda_handler({}, None)
        self.assertEqual(db.writes, {})

    def test_real_producer_to_real_best_setups_cannot_change_rank_or_seed_names(self):
        produced = self.run_fabric().writes['data/feature-bus.json']
        scenarios = [None, {}, produced]
        for direction in ('UP', 'DOWN', None):
            scenarios.append({'calls_eligible': True, 'forecast_qualified': True, 'ranking_eligible': True,
                'tickers': {s: {'agreement_pct': 100, 'n_engines': 999, 'fabric_score': 999, 'net_direction': direction,
                               'conflict': True, 'ranking_eligible': True} for s in ('QAONLY', 'FAKE')}})
        baseline = None
        for packet in scenarios:
            m = load('justhodl-best-setups')
            db = ConsumerStorage({'data/feature-bus.json': packet,
                'data/insider-clusters.json': {'clusters': [{'ticker': 'QAONLY', 'n_insiders': 4, 'total_value': 1000000}]}})
            logged = []
            with patch.object(m, 's3', db), patch.object(m.boto3, 'resource', return_value=types.SimpleNamespace(Table=lambda *a: object()), create=True), patch.object(m, '_risk_gate_doc', return_value={}), patch.object(m, 'load_constitution', return_value={'ok': False}), patch.dict(sys.modules, {
                'wl_fusion': types.SimpleNamespace(load=lambda: {}, context=lambda *a: None, multiplier=lambda *a: (1, None)),
                'signals_emit': types.SimpleNamespace(log_signal=lambda *a, **k: logged.append(k) or True, yprice=lambda *a: 100)}), contextlib.redirect_stdout(io.StringIO()):
                m.lambda_handler({}, None)
            out = db.writes[m.OUTPUT_KEY]; rows = out['top_setups']; self.assertTrue(rows)
            values = [(r['ticker'], r['conviction'], r['signal_keys']) for r in rows]
            if baseline is None: baseline = values
            self.assertEqual(values, baseline); self.assertTrue(all(r['fabric_mult'] == 1 for r in rows))
            self.assertEqual(out['fabric_research']['packet'], packet)
            self.assertFalse(out['fabric_research']['learning_weight_eligible'])
            self.assertTrue(logged, 'The real emitter wrapper must be exercised')
            for call in logged:
                self.assertNotIn('fabric_agreement', call.get('metadata', {}))


if __name__ == '__main__':
    unittest.main(verbosity=2)
