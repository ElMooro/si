"""Actual pure helper and isolated native handler; no AWS/provider calls."""
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
import ast
import json
import math
import unittest

ROOT = Path(__file__).resolve().parents[4]
SOURCE = ROOT / 'aws/lambdas/justhodl-capex-pulse/source/lambda_function.py'


def load(raw, **extra):
    tree = ast.parse(raw)
    wanted = ('aggregate_capex', 'lambda_handler')
    nodes = [n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name in wanted]
    namespace = {'datetime': datetime, 'timezone': timezone, 'json': json,
                 'time': SimpleNamespace(sleep=lambda seconds: None), 'print': lambda *args: None, **extra}
    exec(compile(ast.Module(body=nodes, type_ignores=[]), '<isolated actual Capex>', 'exec'), namespace)
    return namespace


def row(current, prior, count=4):
    return {'current_window_usd': current, 'prior_window_usd': prior,
            'current_window_rows': 4, 'prior_window_rows': count}


class Tests(unittest.TestCase):
    def setUp(self): self.ns = load(SOURCE.read_bytes())

    def test_current_only_names_cannot_inflate_matched_cohort_growth(self):
        result = self.ns['aggregate_capex']([row(10e9, 10e9), row(10e9, None, 1)])
        self.assertEqual(result['capex_ttm_b'], 20)
        self.assertEqual(result['yoy_pct'], 0)
        self.assertEqual(result['comparison_current_usd'], 10e9)
        self.assertEqual(result['comparison_prior_usd'], 10e9)
        self.assertEqual(result['comparison_n'], 1); self.assertEqual(result['comparison_missing_n'], 1)
        self.assertFalse(result['annual_comparability_verified'])

    def test_growth_uses_unrounded_amounts_and_never_divides_by_rounded_minus_100(self):
        result = self.ns['aggregate_capex']([row(1, 1e9)])
        self.assertEqual(result['yoy_pct'], -100)
        self.assertEqual(result['comparison_current_usd'], 1)
        a = self.ns['aggregate_capex']([row(1.234567e9, 1e9), row(4.321234e9, 6e9)])
        self.assertEqual(a['yoy_pct'], round(((1.234567e9+4.321234e9)/7e9-1)*100, 1))

    def test_missing_or_nonpositive_prior_remains_unavailable_and_zero_current_is_valid(self):
        for value in (None, False, '100', 0, -1, float('nan'), float('inf')):
            result = self.ns['aggregate_capex']([row(50, value)])
            self.assertIsNone(result['yoy_pct']); self.assertIsNone(result['comparison_prior_usd'])
        self.assertEqual(self.ns['aggregate_capex']([row(0, 100)])['yoy_pct'], -100)

    def test_invalid_current_and_aggregate_overflow_fail_before_publication(self):
        for value in (None, False, '100', -1, float('nan'), float('inf')):
            with self.subTest(value=value), self.assertRaises(ValueError): self.ns['aggregate_capex']([row(value, 100)])
        with self.assertRaises(ValueError): self.ns['aggregate_capex']([row(1e308, 1e308), row(1e308, 1e308)])

    def native(self, source, *, tiny=False, unavailable=False, amounts=None, currency='USD', fx=1):
        writes = {}
        def cashflows(symbol):
            if unavailable: return None
            count = 8 if symbol == 'PAIRED' else 5
            return [{'date': '2026-06-30', 'reportedCurrency': currency,
                     'capitalExpenditure': amounts[i] if amounts is not None else (-0.25 if tiny and i < 4 else -2.5e9)} for i in range(count)]
        history = {f'original-{i}': {'retained': i} for i in range(405)}
        def read(key, default=None):
            if key == 'data/stock-xray.json': return {'cards': {t: {'mc_b': 100, 'sec': 'Test'} for t in ('PAIRED', 'CURRENT')}}
            if key == 'data/history/capex-pulse.json': return history.copy()
            raise AssertionError('Unreviewed synthetic read: ' + key)
        ns = load(source, _j=read, _fmp_cf=cashflows, _fred_intentions=lambda: None, _usd_per=lambda ccy: (fx, 'synthetic fixture'),
                  N_TOP=160, HYPERSCALERS=['PAIRED', 'CURRENT'], BUCKET='fixture-only',
                  OUT='data/capex-pulse.json', HIST='data/history/capex-pulse.json',
                  s3=SimpleNamespace(put_object=lambda **kw: writes.update({kw['Key']: json.loads(kw['Body'])})))
        ns['lambda_handler']()
        return writes, history

    def test_actual_handler_excludes_partial_prior_from_all_three_comparisons(self):
        writes, history = self.native(SOURCE.read_bytes())
        packet = writes['data/capex-pulse.json']
        for result in (packet['market'], packet['sectors']['Test'], packet['hyperscalers']):
            self.assertEqual(result['yoy_pct'], 0)
            self.assertEqual(result['comparison_n'], 1)
            self.assertEqual(result['comparison_missing_n'], 1)
        current = next(r for r in packet['rows'] if r['ticker'] == 'CURRENT')
        self.assertIsNone(current['yoy_pct']); self.assertIsNone(current['prior_window_usd'])
        self.assertEqual(current['current_window_rows'], 4); self.assertEqual(current['prior_window_rows'], 1)
        self.assertFalse(packet['calls_eligible']); self.assertEqual(packet['quality']['status'], 'partial')
        output_history = writes['data/history/capex-pulse.json']
        self.assertEqual(len(output_history), 406)
        for key, value in history.items(): self.assertEqual(output_history[key], value)

    def test_exact_predecessor_reproduces_false_growth_and_rounded_division_crash(self):
        raw = (ROOT / 'tests/fixtures/pre-capex-pulse-accounting-research.py.txt').read_bytes()
        writes, _ = self.native(raw)
        self.assertEqual(writes['data/capex-pulse.json']['sectors']['Test']['yoy_pct'], 60)
        with self.assertRaises(ZeroDivisionError): self.native(raw, tiny=True)
        writes, _ = self.native(SOURCE.read_bytes(), tiny=True)
        self.assertEqual(writes['data/capex-pulse.json']['market']['yoy_pct'], -100)

    def test_no_usable_sources_cannot_refresh_an_empty_publication_or_history(self):
        writes, _ = self.native(SOURCE.read_bytes(), unavailable=True)
        self.assertEqual(writes, {})

    def test_native_missing_nonfinite_and_boolean_values_are_not_zero_cashflows(self):
        for invalid in (None, False, float('nan'), float('inf')):
            writes, _ = self.native(SOURCE.read_bytes(), amounts=[invalid] + [-1] * 7)
            self.assertEqual(writes, {})
        writes, _ = self.native(SOURCE.read_bytes(), amounts=[0] * 4 + [-1] * 4)
        packet = writes['data/capex-pulse.json']
        self.assertEqual(packet['n'], 2)
        self.assertEqual(packet['market']['capex_ttm_b'], 0)
        self.assertEqual(packet['market']['yoy_pct'], -100)

    def test_invalid_fx_cannot_create_nonfinite_publication(self):
        for rate in (False, -1, float('nan'), float('inf'), 1e308):
            writes, _ = self.native(SOURCE.read_bytes(), currency='EUR', fx=rate)
            self.assertEqual(writes, {})
        writes, _ = self.native(SOURCE.read_bytes(), currency='EUR', fx=0.5)
        self.assertEqual(writes['data/capex-pulse.json']['market']['capex_ttm_b'], 10)


if __name__ == '__main__': unittest.main(verbosity=2)
