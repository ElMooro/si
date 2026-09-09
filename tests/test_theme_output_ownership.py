"""Actual theme writers and consumers retain distinct schemas after deployment."""
from contextlib import redirect_stdout
from copy import deepcopy
import io
import json
import os
from pathlib import Path
import unittest
from unittest.mock import patch
from test_engine_output_ownership import Store, load, ROOT


class ThemeOutputOwnershipTests(unittest.TestCase):
    def test_full_momentum_writer_ignores_legacy_output_overrides(self):
        store = Store()
        with patch.dict(os.environ, {'S3_KEY': 'data/theme-rotation.json', 'STATE_KEY': 'data/theme-rotation.json'}):
            g = load('theme-rotation-engine', store)['lambda_handler'].__globals__
        g['THEMES'] = [('SOXX', 'Semiconductors', 'AI_SEMI', ['NVDA'])]
        g['fetch_history'] = lambda *a, **k: [{'close':100+i, 'volume':1000+i, 'date':str(i)} for i in range(90)]
        g['fetch_etf_holdings'] = lambda *a, **k: [{'symbol':'NVDA', 'weight':1}]
        with redirect_stdout(io.StringIO()), patch('urllib.request.urlopen', side_effect=AssertionError('No network')):
            response = g['lambda_handler']({}, None)
        self.assertEqual(response['statusCode'], 200)
        self.assertEqual([w['Key'] for w in store.writes], ['data/theme-momentum.json', 'data/theme-rotation-state.json'])
        full = json.loads(store.writes[0]['Body'])
        self.assertEqual(full['producer'], 'justhodl-theme-rotation-engine')
        self.assertEqual(full['all_themes'][0]['ticker'], 'SOXX')
        self.assertEqual(full['breadth_details']['SOXX']['constituents_perf'][0]['symbol'], 'NVDA')
        self.assertNotIn('themes', full)

    def test_classifier_success_and_failure_never_replace_curated_watchlists(self):
        store = Store()
        g = load('theme-classifier', store)['lambda_handler'].__globals__
        tickers = ['A','B','C']
        g['load_s3_json'] = lambda *a: {'leaders':[{'ticker': t, 'momentum_score':70+i} for i,t in enumerate(tickers)]}
        g['load_profile_cache'] = lambda: {t:{'industry':'Semiconductors'} for t in tickers}
        with redirect_stdout(io.StringIO()), patch('urllib.request.urlopen', side_effect=AssertionError('No network')):
            response = g['lambda_handler']({}, None)
        self.assertEqual(response['statusCode'], 200)
        full = deepcopy(store.doc)
        self.assertEqual(full['producer'], 'justhodl-theme-classifier')
        self.assertEqual(full['ticker_to_theme']['A'], 'Semiconductors')
        self.assertEqual(full['themes']['Semiconductors']['n_leaders'], 3)
        g['load_s3_json'] = lambda *a: None
        with redirect_stdout(io.StringIO()):
            response = g['lambda_handler']({}, None)
        self.assertEqual(response['statusCode'], 500)
        self.assertTrue(all(w['Key'] == 'data/momentum-themes.json' for w in store.writes))

    def test_actual_consumer_uses_industry_membership_and_full_momentum_fields(self):
        store = Store()
        g = load('velocity-acceleration', store)
        themes = {'ticker_to_theme':{'NVDA':'Semiconductors'}, 'themes':{'Semiconductors':{'n_leaders':3,'label':'Chips','tickers':['NVDA','AMD','AVGO']}}}
        mom = {'leaders':[{'ticker':'NVDA', 'momentum_score':80}]}
        universe = g['build_universe'](mom, themes)
        self.assertIn('NVDA', universe)
        self.assertEqual(universe['NVDA'].get('theme'), 'Semiconductors')
        self.assertEqual(g['THEMES_KEY'], 'data/momentum-themes.json')
        source = ROOT/'aws/lambdas'
        for name in ('theme-second-wave','theme-cascade','theme-cascade-backtest'):
            text = (source/('justhodl-'+name)/'source/lambda_function.py').read_text()
            self.assertIn('data/theme-momentum.json', text)
            self.assertNotIn('data/theme-rotation.json', text)
        self.assertIn('data/theme-rotation.json', (source/'justhodl-fast-filings/source/lambda_function.py').read_text())
        for name in ('catalyst-classifier','catalyst-clusters'):
            self.assertIn('data/momentum-themes.json', (source/('justhodl-'+name)/'source/lambda_function.py').read_text())
        self.assertIn('data/themes.json', (source/'justhodl-market-map/source/lambda_function.py').read_text())


if __name__ == '__main__':
    unittest.main()
