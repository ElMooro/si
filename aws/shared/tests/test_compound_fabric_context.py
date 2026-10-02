"""Whole-handler tests with invented storage only; no provider or account reads."""
from copy import deepcopy
import contextlib
import io
import json
from pathlib import Path
import socket
import sys
import unittest
from unittest.mock import patch

sys.path[:0] = [str(Path(__file__).resolve().parents[1]), str(Path(__file__).resolve().parent)]
from ciss_vintage_test_support import load
from compound_research_context import project, read, pointers, BASIS, SOURCE
from test_compound_numeric import Storage


class Tests(unittest.TestCase):
    def setUp(self):
        guard = patch.object(socket.socket, 'connect', side_effect=AssertionError('Offline only'))
        guard.start(); self.addCleanup(guard.stop)

    def fabric(self, packet, extras=None):
        db = Storage({SOURCE: packet, 'data/trend-reversal.json': {
            'rows': [{'ticker': 'QAONLY', 'reversal_score': 30, 'direction': 'BOTTOM_FORMING'}]}, **(extras or {})})
        m = load('justhodl-signal-fabric')
        # Exercise the actual S3 reader for Compound; all other established
        # sources also come only from the invented in-memory dictionary.
        with patch.object(m, 's3', db), patch.object(m, 'rd', side_effect=lambda key: deepcopy(db.objects.get(key))), contextlib.redirect_stdout(io.StringIO()):
            m.lambda_handler({}, None)
        for value in db.writes.values():
            json.dumps(value, allow_nan=False)
        return db.writes

    def test_compound_cannot_add_direction_weight_confidence_or_universe_even_with_forged_flags(self):
        baseline = self.fabric({})
        packet = {'calls_eligible': True, 'forecast_qualified': True, 'compound': [
            {'symbol': s, 'n_systems': 6000, 'desk_score': score, 'independence_eligible': True}
            for s, score in [('QAONLY', 10**6), ('FAKE', -10**6)]]}
        out = self.fabric(packet)
        a, b = baseline['data/signal-fabric.json'], out['data/signal-fabric.json']
        self.assertEqual(a['tickers'], b['tickers']); self.assertEqual(b['source_stats']['compound-aggregator'], 0)
        self.assertEqual(b['compound_context']['packet'], packet)
        self.assertEqual(out['data/feature-bus.json']['tickers']['QAONLY']['compound_context_pointers'], ['/compound/0'])
        self.assertIsNone(out['data/feature-bus.json']['tickers']['QAONLY']['compound'])
        self.assertNotIn('FAKE', out['data/feature-bus.json']['tickers'])
        self.assertIsNone(load('justhodl-signal-fabric').st_compound(packet['compound'][0]))

    def test_actual_compound_handler_packet_survives_consumer_with_full_calculation(self):
        m = load('justhodl-compound-aggregator')
        db = Storage({'data/nobrainers.json': {'summary': {'top_25_overall': [{'ticker': 'QAONLY', 'score': 0}]}},
                      'data/insider-clusters.json': {'clusters': [{'ticker': 'QAONLY', 'score': 10}]}})
        with patch.object(m, 'S3', db), patch.object(m, 'emit_alerts', side_effect=AssertionError('No sends')), contextlib.redirect_stdout(io.StringIO()):
            m.lambda_handler({'suppress_alerts': True})
        packet = db.writes[m.S3_KEY]
        out = self.fabric(packet)['data/signal-fabric.json']
        context = out['compound_context']; self.assertEqual(context['packet'], packet)
        self.assertEqual(context['occurrences'][0]['record']['compound_score'], 15)
        self.assertEqual(context['occurrences'][0]['record']['score_calculation'], packet['compound'][0]['score_calculation'])
        self.assertEqual(out['tickers'][0]['n_engines'], 1)
        self.assertFalse(out['tickers'][0]['engines'][0]['independence_eligible'])
        self.assertEqual(out['tickers'][0]['engines'][0]['ancestry_status'], 'not_traced')

    def test_all_occurrences_beyond_legacy_cap_and_duplicate_records_are_preserved(self):
        rows = [{'symbol': 'QAONLY', 'desk_score': i, 'unknown': [i, None]} for i in range(650)]
        packet = {'compound': rows, 'unmodeled': {'keep': True}}
        out = self.fabric(packet)['data/signal-fabric.json']['compound_context']
        self.assertEqual(out['selected_count'], 650)
        self.assertEqual([r['record'] for r in out['occurrences']], rows)
        self.assertEqual(pointers(out, 'QAONLY')[-1], '/compound/649')
        self.assertEqual(out['packet'], packet)

    def test_canonical_absent_allows_legacy_context_but_present_null_empty_or_malformed_never_revives_it(self):
        old = [{'symbol': 'OLD', 'desk_score': 0}]
        self.assertEqual(project({'ranked': old})['occurrences'][0]['pointer'], '/ranked/0')
        for value in (None, [], {}, True):
            out = project({'compound': value, 'ranked': old})
            self.assertEqual(out['occurrences'], [])
            self.assertEqual(out['packet']['ranked'], old)

    def test_ambiguous_identity_cannot_join_but_original_record_remains(self):
        rows = [None, {'ticker': 'A', 'symbol': 'B'}, {'symbol': 1}, {'symbol': ' qaonly '}]
        context = project({'compound': rows}); self.assertEqual([r['record'] for r in context['occurrences']], rows)
        self.assertEqual(pointers(context, 'QAONLY'), ['/compound/3'])
        self.assertEqual(pointers(context, 'A'), [])

    def test_failure_does_not_leak_and_whole_source_validation_precedes_context(self):
        for value in (PermissionError('PRIVATE_ERROR'), b'{"compound":[],"compound":[1]}', b'{"compound":[NaN]}'):
            db = Storage({SOURCE: value}); out = read(db, 'invented')
            self.assertEqual(db.reads, [SOURCE]); self.assertEqual(out['status'], 'unavailable')
            self.assertNotIn('PRIVATE_ERROR', json.dumps(out)); self.assertFalse(out['source_qualified'])
        class Truncated(Storage):
            def get_object(self, **kw):
                response = super().get_object(**kw); response['ContentLength'] += 1
                self.body = response['Body']; return response
        db = Truncated({SOURCE: {'compound': []}})
        self.assertEqual(read(db, 'invented')['status'], 'unavailable'); self.assertTrue(db.body.closed)

    def test_calculation_revision_does_not_create_direction_flip_or_consensus_events(self):
        previous = {'tickers': {'QAONLY': {'net_direction': 'DOWN', 'agreement_pct': 0}}}
        out = self.fabric({}, {'data/feature-bus.json': previous})
        events = out['data/fabric-events.json']; self.assertEqual(events['events'], [])
        self.assertFalse(events['prior_calculation_comparable'])
        previous['compound_research_boundary'] = BASIS
        out = self.fabric({}, {'data/feature-bus.json': previous})
        # A matching Compound revision still does not qualify source-vintage comparability.
        self.assertEqual(out['data/fabric-events.json']['events'], [])
        self.assertFalse(out['data/fabric-events.json']['prior_calculation_comparable'])

    def test_every_published_projection_preserves_boundary_without_granting_forecast_authority(self):
        for key, packet in self.fabric({'compound': []}).items():
            self.assertEqual(packet['compound_research_boundary'], BASIS, key)
            for field in ('calls_eligible', 'sizing_eligible', 'forecast_qualified'):
                self.assertFalse(packet[field], (key, field))


if __name__ == '__main__':
    unittest.main(verbosity=2)
