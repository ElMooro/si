"""Offline count-validity regression gate; no credentials, AWS or provider reads."""
import ast
import json
import math
import statistics
import contextlib
import io
import re
import sys
import unittest
from datetime import datetime, timezone
from types import SimpleNamespace
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

HERE = Path(__file__).resolve().parent
SOURCE = HERE.parent / 'source/lambda_function.py'


def load(path):
    tree = ast.parse(path.read_text(encoding='utf-8'))
    # Extract functions without module initialization (managed_secret/boto3).
    ns = dict(math=math, statistics=statistics, ThreadPoolExecutor=ThreadPoolExecutor,
              as_completed=as_completed, MIN_DOLLAR_VOL=5_000_000)
    exec(compile(ast.Module(body=[n for n in tree.body if isinstance(n, ast.FunctionDef)],
                            type_ignores=[]), str(path), 'exec'), ns)
    return ns


NEW = load(SOURCE)
OLD = load(HERE / 'legacy-before.py.txt')
BAR = dict(v=1_000_000, n=10_000, vw=100, o=100, c=100, h=101, l=99)
BASE = dict(avg_vol=1_000_000, avg_dollar_vol=100_000_000, avg_range_pct=.02,
            avg_n_txns=10_000, avg_trade_size_baseline=100)
BAD_COUNTS = [None, 0, -1, True, False, float('nan'), float('inf'),
              -float('inf'), '10000', '', [], {}, 1.5]


def packet():
    ns = load(SOURCE)
    written = []
    ns.update(json=json, datetime=SimpleNamespace(now=lambda tz: datetime(2026, 10, 1, 22, tzinfo=tz)), timezone=timezone,
              time=SimpleNamespace(time=lambda: 0), N_BASELINE_DAYS=20,
              BUCKET='offline', S3_KEY_OUT='data/tape-reader.json',
              S3=SimpleNamespace(put_object=lambda **kw: written.append(kw)))
    rows = {'VALID': dict(BAR, v=3_000_000, n=1_000),
            'UNKNOWN': dict(BAR, v=3_000_000, n=None)}
    ns['fetch_universe'] = lambda: list(rows)
    ns['fetch_most_recent_today'] = lambda: ('2026-09-30', rows)
    ns['build_baseline'] = lambda *a, **kw: {k: BASE for k in rows}
    with contextlib.redirect_stdout(io.StringIO()):
        ns['lambda_handler']({}, None)
    assert len(written) == 1 and written[0]['Key'] == 'data/tape-reader.json'
    return json.loads(written[0]['Body'])


class CountValidity(unittest.TestCase):
    def score(self, n, bar=None, base=None):
        row = dict(bar or BAR)
        if n == 'missing':
            row.pop('n', None)
        else:
            row['n'] = n
        return NEW['score_ticker'](row, base or BASE)

    def test_frozen_before_after_cases(self):
        # Frozen arithmetic, including unchanged non-size terms and valid size term.
        cases = [('normal', 10_000, 1_000_000, 0, 0),
                 ('large_mean', 1_000, 1_000_000, 25, 25),
                 ('missing', None, 1_000_000, 25, 0),
                 ('zero_count', 0, 1_000_000, 25, 0),
                 ('surge_missing_count', None, 3_000_000, 79, 54)]
        for name, n, v, before, after in cases:
            with self.subTest(name=name):
                row = dict(BAR, n=n, v=v)
                prior = OLD['score_ticker'](row, BASE)
                current = NEW['score_ticker'](row, BASE)
                self.assertEqual(prior[0], before)
                self.assertEqual(current[0], after)
                self.assertEqual(before - after, 25 if n in (None, 0) else 0)
                for k in ('rel_volume', 'rel_dollar_volume', 'range_expansion',
                          'today_vol', 'today_dollar_vol'):
                    self.assertEqual(prior[1][k], current[1][k])
                self.assertNotIn('BLOCK_PRINTS', current[2])
                self.assertNotIn('block prints', NEW['synth_rationale']('TEST', current[1], current[2], current[3]))

    def test_missing_zero_negative_boolean_nonfinite_counts(self):
        for n in ['missing'] + BAD_COUNTS:
            with self.subTest(n=repr(n)):
                score, fields, tags, _ = self.score(n)
                self.assertEqual(score, 0)
                self.assertIsNone(fields['avg_trade_size_today'])
                self.assertIsNone(fields['block_ratio'])
                self.assertEqual(fields['trade_size_score'], 0)
                self.assertEqual(fields['trade_size_status'], 'unavailable')
                self.assertNotIn('LARGE_AVG_TRADE_SIZE', tags)
                self.assertEqual(fields['today_n_trades'], 0 if type(n) is int and n == 0 else None)
                json.dumps(fields, allow_nan=False)

    def test_strict_volume_and_zero_are_distinct(self):
        self.assertEqual(NEW['average_trade_size'](0, 10), 0)
        self.assertIsNone(NEW['average_trade_size'](0, 0))
        self.assertIsNone(NEW['average_trade_size'](None, 10))
        for v in [None, True, False, '0', -1, float('nan'), float('inf'), 10**400]:
            self.assertIsNone(NEW['average_trade_size'](v, 10))
            self.assertIsNone(NEW['score_ticker'](dict(BAR, v=v), BASE)[1])
        # Existing liquidity filter still excludes a measured zero-volume session.
        self.assertEqual(NEW['score_ticker'](dict(BAR, v=0), BASE)[:2], (0, None))

    def baseline(self, bars):
        ns = load(SOURCE)
        ns['get_baseline_dates'] = lambda n: list(range(len(bars)))
        ns['fetch_grouped_daily'] = lambda day: {'TEST': bars[day]}
        return ns['build_baseline'](20, {'TEST'})['TEST']

    def test_baseline_missingness_cannot_be_averaged_away(self):
        valid = self.baseline([BAR.copy() for _ in range(5)])
        self.assertEqual(valid['avg_trade_size_baseline'], 100)
        for n in BAD_COUNTS:
            with self.subTest(n=repr(n)):
                bars = [BAR.copy() for _ in range(5)]
                bars[0]['n'] = n
                base = self.baseline(bars)
                self.assertIsNone(base['avg_trade_size_baseline'])
                self.assertEqual(base['avg_vol'], valid['avg_vol'])
                self.assertEqual(base['avg_dollar_vol'], valid['avg_dollar_vol'])
                self.assertEqual(base['avg_range_pct'], valid['avg_range_pct'])
                row = NEW['score_ticker'](dict(BAR, v=3_000_000), base)
                self.assertEqual(row[0], 54)
                self.assertIsNone(row[1]['block_ratio'])

    def test_valid_zero_and_fractional_volume_baselines(self):
        bars = [dict(BAR, v=0, n=10) for _ in range(5)]
        self.assertEqual(self.baseline(bars)['avg_trade_size_baseline'], 0)
        self.assertEqual(NEW['average_trade_size'](.5, 2), .25)
        self.assertIsNone(self.score(10, base=dict(BASE, avg_trade_size_baseline=0))[1]['block_ratio'])
        for v in [None, True, '100', -1, float('inf'), float('nan')]:
            self.assertIsNone(self.score(10, base=dict(BASE, avg_trade_size_baseline=v))[1]['block_ratio'])

    def test_finite_inputs_with_nonfinite_sum_do_not_make_size(self):
        bars = [dict(BAR, v=1e308, n=1, vw=1, c=1) for _ in range(5)]
        self.assertIsNone(self.baseline(bars)['avg_trade_size_baseline'])
        bars = [dict(BAR, n=1e308) for _ in range(5)]
        self.assertIsNone(self.baseline(bars)['avg_trade_size_baseline'])

    def test_source_calls_dates_and_handler_call_sites_unchanged(self):
        old = ast.parse((HERE / 'legacy-before.py.txt').read_text(encoding='utf-8'))
        new = ast.parse(SOURCE.read_text(encoding='utf-8'))
        names = ['_http_get_json', 'fetch_grouped_daily', 'fetch_most_recent_today',
                 'get_baseline_dates', 'fetch_universe']
        for name in names:
            a = next(n for n in old.body if isinstance(n, ast.FunctionDef) and n.name == name)
            b = next(n for n in new.body if isinstance(n, ast.FunctionDef) and n.name == name)
            self.assertEqual(ast.dump(a), ast.dump(b), name)
        for name in ['build_baseline', 'lambda_handler']:
            def calls(tree):
                fn = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == name)
                return [ast.dump(n) for n in ast.walk(fn) if isinstance(n, ast.Call)
                        and isinstance(n.func, ast.Name) and n.func.id in
                        ('fetch_grouped_daily', 'fetch_most_recent_today', 'fetch_universe', 'build_baseline')]
            self.assertEqual(calls(old), calls(new))

    def test_actual_handler_packet_and_generic_decision_abstention(self):
        payload = packet()
        self.assertEqual(payload['measurement_contract'], 'tape-reader-activity.v2')
        self.assertEqual([r['score'] for r in payload['top_loud_tape']], [79, 54])
        self.assertIsNone(payload['top_loud_tape'][1]['block_ratio'])
        json.dumps(payload, allow_nan=False)
        root = HERE.parents[3]
        for engine, functions, assignments, namespace, entry in [
            ('justhodl-strategist', {'classify','scan_text','_verdict_from','_ticks','extract'},
             {'POS','NEG','NEU','VERDICT_KEYS','TEXT_KEYS','TEXT_POS','TEXT_NEG','SCORE_KEYS','PICK_KEYS','CONTAINERS','_TICK'},
             {'re': re}, 'extract'),
            ('justhodl-causality-scanner', {'extract_scalar'}, {'NUMERIC_KEYS'}, {'math': math}, 'extract_scalar')]:
            path = root / 'aws/lambdas' / engine / 'source/lambda_function.py'
            tree = ast.parse(path.read_text(encoding='utf-8'))
            selected = [n for n in tree.body if
                        (isinstance(n, ast.FunctionDef) and n.name in functions) or
                        (isinstance(n, ast.Assign) and any(isinstance(t, ast.Name) and t.id in assignments for t in n.targets))]
            exec(compile(ast.Module(body=selected, type_ignores=[]), str(path), 'exec'), namespace)
            self.assertIsNone(namespace[entry](payload), engine)


if __name__ == '__main__':
    if sys.argv[1:] == ['--fixture']:
        print(json.dumps(packet(), allow_nan=False))
    else:
        unittest.main()
