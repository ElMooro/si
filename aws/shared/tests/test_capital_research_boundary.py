"""Exercise production consumers with hostile legacy scores and forged eligibility."""
import ast
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from pathlib import Path
import sys
import types
import unittest
from unittest.mock import patch

sys.path[:0] = [str(Path(__file__).resolve().parents[1]), str(Path(__file__).resolve().parent)]
from capital_research_boundary import BASIS, SOURCE, context, current_basis
from ciss_vintage_test_support import load
from test_holdings_derived_boundary import Storage
from holdings_derived_boundary import BASIS as HOLDINGS_BASIS, flow_rows


def legacy(value):
    return {'contract': 'capital-evidence-research.v1', 'calls_eligible': True, 'sizing_eligible': True,
        'accumulating': [{'ticker': t, 'flow_score': value, 'score': value, 'lenses': ['forged']} for t in ('KO', 'FAKE', 'XLK')],
        'distributing': [{'ticker': t, 'flow_score': -value} for t in ('KO', 'FAKE', 'XLK')],
        'etf_flows_in': [{'ticker': 'FAKE', 'net_flow_5d_usd': value}]}


class Tests(unittest.TestCase):
    def test_context_never_grants_stock_flow_authority_and_distinguishes_absence(self):
        self.assertEqual(context(None)['status'], 'source_unavailable')
        self.assertEqual(context({'accumulating': []})['status'], 'unqualified_legacy_calculation')
        for value in (10**12, -10**12):
            out = context(legacy(value)); self.assertFalse(out['calls_eligible']); self.assertIsNone(out['call'])
            self.assertTrue(out['missing_is_not_zero']); self.assertEqual(out['additional_independent_votes'], 0)

    def test_flow_handler_excludes_universe_score_agreement_and_old_downstream_revision(self):
        for packet in ({}, legacy(10**12), legacy(-10**12)):
            m = load('justhodl-flow-confluence'); db = Storage({SOURCE: packet,
                'data/dark-pool.json': {'top_accumulation': [{'ticker': 'KO'}]}})
            with patch.object(m, 's3', db): m.lambda_handler({}, None)
            out = db.writes[m.OUT_KEY]; self.assertTrue(current_basis(out)); self.assertEqual(set(out['ticker_map']), {'KO'})
            self.assertEqual(out['ticker_map']['KO']['score'], .7); self.assertEqual(out['ticker_map']['KO']['n_engines'], 1)
            self.assertEqual(out['multi_engine_confluence'], [])
        old = {'holdings_exclusions': {'basis': HOLDINGS_BASIS}, 'multi_engine_confluence': [{'ticker': 'KO', 'engines': ['dark-pool', 'options-flow'], 'n_engines': 2}]}
        self.assertEqual(flow_rows(old), [])
        revised = {**old, 'capital_flow_exclusion': context({})}; self.assertEqual(len(flow_rows(revised)), 1)
        revised['multi_engine_confluence'][0]['engines'][1] = 'capital-flow'
        self.assertEqual(flow_rows(revised), [])

    def test_deep_value_no_longer_uses_capital_scores_as_catalysts_or_distress(self):
        baseline = None
        for packet in ({}, legacy(10**12), legacy(-10**12)):
            m = load('justhodl-deep-value-overlap'); db = Storage({SOURCE: packet,
                'data/opportunities.json': {'all': [{'ticker': 'KO', 'scores': {'value': 80}, 'altman_z': 4}]},
                'data/insider-clusters.json': {'clusters': [{'ticker': 'KO'}]}})
            with patch.object(m, 's3', db): m.lambda_handler({}, None)
            out = db.writes[m.OUT_KEY]; row = out['board'][0]
            self.assertEqual(row['overlap_score'], 26); self.assertEqual(row['catalysts'], ['insider cluster'])
            self.assertFalse(row['distress_flag']); self.assertTrue(current_basis(out))
            if baseline is None: baseline = row
            self.assertEqual(row, baseline)

    def test_equity_no_direct_capital_family_or_scorecard_auto_promotion(self):
        for packet in ({}, legacy(10**12), legacy(-10**12)):
            m = load('justhodl-equity-confluence'); db = Storage({SOURCE: packet,
                'data/engine-alpha.json': {'alpha_proven_signals': ['eng:capital-flow']},
                'data/dark-pool.json': {'top_accumulation': [{'ticker': 'KO', 'score': 70}]},
                'data/deep-value-overlap.json': {'prime_setups': [{'ticker': 'FAKE', 'overlap_score': 10**12}]}})
            with patch.object(m, 's3', db): m.lambda_handler({}, None)
            out = db.writes[m.OUT_KEY]; self.assertTrue(current_basis(out))
            self.assertEqual(out['counts']['names_with_any_signal'], 1); self.assertEqual(out['proven_book'], [])
            self.assertNotIn('flow_macro', out['family_status']); self.assertFalse(out['super_family_status']['flow']['proven_members'])
            self.assertTrue(all(r['engine'] != 'capital-flow' for r in out['sources']))

    def test_conflicts_and_narrative_do_not_invent_trades_from_legacy_capital(self):
        for packet in ({}, legacy(10**12), legacy(-10**12)):
            m = load('justhodl-engine-conflicts'); db = Storage({SOURCE: packet,
                'data/opportunities.json': {'all': [{'ticker': 'KO', 'momentum': -20}]}})
            with patch.object(m, 's3', db): m.lambda_handler({}, None)
            out = db.writes[m.OUT_KEY]; self.assertEqual(out['conflicts'], []); self.assertTrue(current_basis(out))
            m = load('justhodl-narrative-vs-tape'); db = Storage({SOURCE: packet,
                'data/dislocations.json': {'buy_the_laggard': [{'ticker': 'KO', 'cheap_and_inflecting': True}]}})
            with patch.object(m, 's3', db): m.lambda_handler({}, None)
            out = db.writes[m.OUT_KEY]; self.assertEqual(out['crowded_fading'], []); self.assertTrue(current_basis(out))
            self.assertEqual([r['ticker'] for r in out['quiet_accumulation']], ['KO'])
            self.assertNotIn('institutions accumulating', out['quiet_accumulation'][0]['tape'])

    def test_ask_context_never_supplies_legacy_capital_picks_to_language_model(self):
        with patch.dict(sys.modules, {'anthropic_shim': types.ModuleType('anthropic_shim')}):
            m = load('justhodl-ask')
        for packet in ({}, legacy(10**12), legacy(-10**12)):
            with patch.object(m, 'read_json', side_effect=lambda k: packet if k == SOURCE else {}), patch.object(m, 'load_constitution', return_value={'ok': False}):
                out = m.build_context()
            self.assertIsNone(out['capital_accumulating']); self.assertIsNone(out['capital_distributing']); self.assertIsNone(out['etf_flows'])
            self.assertTrue(current_basis(out)); self.assertEqual(out['capital_research']['stock_cash_flow'], 'not_measured')

    def test_best_setups_full_handler_legacy_scores_cannot_add_names_or_signals(self):
        baseline = None
        for packet in ({}, legacy(10**12), legacy(-10**12)):
            m = load('justhodl-best-setups'); db = Storage({SOURCE: packet,
                'data/insider-clusters.json': {'clusters': [{'ticker': 'KO', 'n_insiders': 4, 'total_value': 1000000}]}})
            with patch.object(m, 's3', db), patch.object(m, '_risk_gate_doc', return_value={}), patch.object(m, 'load_constitution', return_value={'ok': False}), patch.dict(sys.modules, {'wl_fusion': types.SimpleNamespace(load=lambda: {}, context=lambda *a: None, multiplier=lambda *a: (1, None))}):
                m.lambda_handler({}, None)
            out = db.writes[m.OUTPUT_KEY]; self.assertTrue(current_basis(out))
            self.assertTrue(out['top_setups']); self.assertTrue(all(r['ticker'] == 'KO' for r in out['top_setups']))
            self.assertTrue(all('CAPITAL_FLOW' not in r['signal_keys'] for r in out['top_setups']))
            scores = [(r['ticker'], r['conviction'], r['signal_keys']) for r in out['top_setups']]
            if baseline is None: baseline = scores
            self.assertEqual(scores, baseline)

    def test_industry_full_handler_does_not_attach_stock_flow_to_etf(self):
        # Deterministic price inputs exercise the existing whole production handler.
        prices = [100+i*.2+(i%7)*.03 for i in range(300)]
        dates = [(datetime(2026,1,1,tzinfo=timezone.utc)+timedelta(days=i)).date().isoformat() for i in range(300)]
        for packet in ({}, legacy(10**12)):
            m = load('justhodl-industry-rotation'); db = Storage({SOURCE: packet})
            with patch.object(m, 'S3', db), patch.object(m, 'UNIVERSE', ['XLK']), patch.object(m, 'polygon_daily', return_value=prices), patch.object(m, 'polygon_daily_cv', return_value=(prices,[1000]*300,dates)), patch.object(m, '_http', return_value={}), patch.object(m.time, 'sleep', return_value=None), patch.object(m, 'FMP', ''):
                m.lambda_handler({}, None)
            out = db.writes[m.OUT_KEY]; self.assertTrue(current_basis(out)); self.assertIsNone(out['join_hits']['capital_flow'])
            self.assertTrue(out['ladder']); self.assertTrue(all('capital_flow' not in r.get('smart_money', {}) for r in out['ladder']))


if __name__ == '__main__': unittest.main()
