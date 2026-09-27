"""Isolate actual consumer functions from credentials, outputs and notifications."""
from copy import deepcopy
from pathlib import Path
from typing import Dict, List, Optional
import ast
import sys
import unittest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'aws/shared'))
import sec_search_research as boundary


def functions(family, names, namespace):
    source = (ROOT / 'aws/lambdas' / ('justhodl-' + family) / 'source/lambda_function.py').read_bytes()
    nodes = [n for n in ast.parse(source).body if isinstance(n, ast.FunctionDef) and n.name in names]
    if {n.name for n in nodes} != set(names) or any(n.decorator_list for n in nodes):
        raise ValueError('Exact isolated undecorated functions required')
    exec(compile(ast.Module(body=nodes, type_ignores=[]), '<isolated consumer>', 'exec'), namespace)
    return namespace


class Tests(unittest.TestCase):
    def test_boundary_preserves_other_families_and_does_not_mutate_packet(self):
        packet = {'all_tickers': [{'ticker': 'TEST', 'score': 100}], 'calls_eligible': True}
        before = deepcopy(packet)
        self.assertEqual(boundary.guard(boundary.KEY, packet), {})
        self.assertIs(boundary.guard('data/unrelated.json', packet), packet)
        self.assertEqual(packet, before); self.assertFalse(boundary.context()['calls_eligible'])
        self.assertEqual(boundary.context()['independent_investment_votes'], 0)

    def test_sec_records_cannot_supply_convergence_or_directional_votes(self):
        ns = functions('convergence-radar', ['extract_ticker_signals_from_engine', '_direction_sec_filings'], {})
        for flags in ({}, {'calls_eligible': False}, {'calls_eligible': True, 'forecast_qualified': True}):
            row = {'ticker': 'TEST', 'bullish_signals': 2, 'bearish_signals': 0, 'highest_severity': 'high', **flags}
            self.assertEqual(ns['extract_ticker_signals_from_engine']('sec-filings-intel', {}, [row]), {})
            self.assertEqual(ns['extract_ticker_signals_from_engine']('alias', {'key': boundary.KEY}, [row]), {})
            self.assertEqual(ns['_direction_sec_filings'](row)[0], 0.0)

    def test_abstention_is_absent_from_direction_denominator(self):
        ns = functions('convergence-radar', ['compute_directional_score'],
                       {'Dict': Dict, 'DIRECTION_FN': {'options-flow': lambda sig: (0.5, 'synthetic qualified other family')}})
        base = ns['compute_directional_score']({'options-flow': {}})
        combined = ns['compute_directional_score']({'options-flow': {}, 'sec-filings-intel': {'bullish_signals': 1000}})
        self.assertEqual(base, combined)
        self.assertEqual(combined['n_neutral_eng'], 0)
        self.assertEqual([c['engine'] for c in combined['contributions']], ['options-flow'])

    def test_new_alias_cannot_activate_ownership_or_squeeze_bonus(self):
        ns = functions('pump-mechanics', ['extract_concentration_signals', 'compute_squeeze_score'],
                       {'Optional': Optional, 'List': List, 'Dict': Dict})
        packet = {'calls_eligible': True, 'events': [{'type': '13D', 'description': 'Unverified keyword'},
                                                   {'type': 'INSIDER', 'direction': 'BUY'}]}
        signals = ns['extract_concentration_signals'](packet)
        self.assertEqual(signals, [])
        self.assertEqual(ns['compute_squeeze_score'](0, None, {}, None, len(signals)), 0)

    def test_nested_and_top_level_sec_population_aliases_cannot_blacklist(self):
        state = {'data/beneish.json': {'manipulators': [{'ticker': 'KEEP'}]}}
        ns = functions('best-ideas', ['forensic_flags'], {'_load_json': lambda key: state.get(key, {})})
        event = {'ticker': 'TEST', 'severity': 'critical', 'calls_eligible': True}
        for packet in ({'critical': [event]}, {'events': [event]}, {'highlights': {'critical': [event]}}):
            state[boundary.KEY] = packet
            bad, notes = ns['forensic_flags']()
            self.assertEqual(bad, {'KEEP'}); self.assertEqual(notes, {'KEEP': 'Beneish manipulation flag'})


def run():
    result = unittest.TextTestRunner(verbosity=2).run(unittest.defaultTestLoader.loadTestsFromTestCase(Tests))
    if not result.wasSuccessful(): raise SystemExit(1)
