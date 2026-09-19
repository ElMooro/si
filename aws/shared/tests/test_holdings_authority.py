"""Actual consumer paths must not turn holdings values into trading votes."""
import ast
from copy import deepcopy
import io
import json
from pathlib import Path
import sys
import types
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[3]
sys.path[:0] = [str(ROOT/'aws/shared'), str(Path(__file__).resolve().parent)]
from holdings_authority import context
from ciss_vintage_test_support import load


def packet(value=10**12):
    return {'generated_at': '2026-09-19T20:00:00Z', 'calls_eligible': True,
            'sizing_eligible': True, 'vote_eligible': True,
            'most_bought': [{'ticker': 'KO', 'n_funds_adding': 100, 'n_funds_new_position': 100}],
            'by_fund': {'FIXTURE': {'changes_summary': {'n_new': 999, 'exits': []}}},
            't': {'KO': {'n': value, 'b': value, 'na': 999}}}


class Tests(unittest.TestCase):
    def test_untrusted_flags_and_extreme_values_never_grant_authority(self):
        for p in (None, {}, [], packet(), packet(-10**12), {'contract': 'holdings-native-research.v1', **packet()}):
            before = deepcopy(p); out = context(p)
            self.assertFalse(out['vote_eligible']); self.assertFalse(out['calls_eligible'])
            self.assertFalse(out['sizing_eligible']); self.assertIsNone(out['score'])
            self.assertEqual(out['independent_votes'], 0); self.assertEqual(p, before)

    def test_actual_ranker_index_excludes_holdings_but_retains_source_context(self):
        m = load('justhodl-master-ranker'); m.engine_trust = None
        base = {'compound': [{'symbol': 'KO', 'compound_score': 50, 'n_systems': 1}]}
        baseline = None
        for p in (None, packet(), packet(-10**12)):
            with patch.object(m, 'fetch_json', side_effect=lambda key, **kw: p if key == 'data/13f-positions.json' else base if key == 'data/compound-signals.json' else {}):
                idx, feeds = m.build_ticker_index()
            self.assertEqual(set(idx['KO']), {'compound'})
            self.assertIs(feeds['institutional_13f'], p)
            self.assertFalse(feeds['institutional_13f_context']['vote_eligible'])
            score = m.compute_conviction(idx['KO'], {})
            if baseline is None: baseline = score
            self.assertEqual(score, baseline)
            self.assertEqual(len(score[1]), 1)
        with patch.object(m, 'fetch_json', side_effect=lambda key, **kw: packet() if key == 'data/13f-positions.json' else {}):
            idx, _ = m.build_ticker_index()
        self.assertEqual(idx, {})

    def test_actual_regime_fetch_excludes_holdings_from_dimension_denominator(self):
        m = load('justhodl-regime-composite')
        cfg = next(c for c in m.MODULES_CFG if c.get('derive') == 'thirteenf')
        client = types.SimpleNamespace(get_object=lambda **kw: {'Body': io.BytesIO(json.dumps(packet()).encode())})
        with patch.object(m, 'S3', client): row = m.fetch_module(cfg)
        self.assertFalse(row['vote_eligible']); self.assertIsNone(row['polarity'])
        self.assertIsNone(m.derive_thirteenf(packet())[2])
        good = {'dimension': cfg['dimension'], 'label': 'independent fixture', 'regime': 'fixture', 'polarity': -1, 'missing': False}
        dim = m.compute_dimensions([row, good])[cfg['dimension']]
        self.assertEqual(dim['n'], 1); self.assertEqual(dim['score'], -1)
        self.assertEqual(m.compute_dimensions([row])[cfg['dimension']]['n'], 0)

    def test_actual_fabric_handler_excludes_flow_and_feature_without_diluting_agreement(self):
        m = load('justhodl-signal-fabric'); baseline = None
        for p in (None, packet(), packet(-10**12)):
            objects = {'data/trend-reversal.json': {'rows': [{'ticker': 'KO', 'reversal_score': 30, 'direction': 'BOTTOM_FORMING'}]},
                       'data/13f-flows-by-ticker.json': p}
            writes = {}
            client = types.SimpleNamespace(put_object=lambda **kw: writes.update({kw['Key']: json.loads(kw['Body'])}))
            with patch.object(m, 'rd', side_effect=lambda key: objects.get(key)), patch.object(m, 's3', client):
                m.lambda_handler({}, None)
            out = writes['data/signal-fabric.json']; bus = writes['data/feature-bus.json']
            self.assertEqual(out['source_stats']['13f-flows'], 0)
            self.assertFalse(out['holdings_context']['vote_eligible'])
            self.assertIsNone(bus['tickers']['KO']['flow_13f'])
            self.assertEqual(out['tickers'][0]['n_engines'], 1)
            self.assertEqual(out['tickers'][0]['agreement_pct'], 100)
            row = out['tickers'][0]
            if baseline is None: baseline = row
            self.assertEqual(row, baseline)
            self.assertIsNone(m.st_13f('KO', packet()['t']))

    def test_public_ranker_refuses_private_notes_before_any_storage_read(self):
        m = load('justhodl-master-ranker'); reads = []
        client = types.SimpleNamespace(get_object=lambda **kw: reads.append(kw) or {'Body': io.BytesIO(b'{"fixture":true}')})
        with patch.object(m, 'S3', client):
            for key in ('data/notes-index.json', 'data/brain.json', 'portfolio/snapshot.json', 'data/portfolio/risk.json', 'data/history/archive/feed/notes-index.json/fixture.json'):
                self.assertIsNone(m.fetch_json(key))
            self.assertEqual(reads, [])
            self.assertEqual(m.fetch_json('data/13f-positions.json'), {'fixture': True})
        self.assertEqual(len(reads), 1)
        self.assertTrue(all(not f['used'] for f in m._FEED_HEALTH[:-1]))

    def test_explicit_ranker_event_suppression_executes_actual_publish_boundary(self):
        source = (ROOT/'aws/lambdas/justhodl-master-ranker/source/lambda_function.py').read_text(encoding='utf-8')
        tree = ast.parse(source)
        node = next(n for n in ast.walk(tree) if isinstance(n, ast.If) and 'suppress_events' in ast.unparse(n.test))
        for event, expected in (({}, 2), ({'suppress_events': True}, 0), ({'suppress_events': False}, 2)):
            calls = []; scope = {'event': event, 'tier_events': list(range(11)), 'publish_many': calls.append, 'emitted_tier_events': 0}
            exec(compile(ast.Module(body=[node], type_ignores=[]), '<actual event boundary>', 'exec'), scope)
            self.assertEqual(len(calls), expected)
            self.assertEqual(scope['emitted_tier_events'], 0 if not expected else 11)


if __name__ == '__main__': unittest.main()
