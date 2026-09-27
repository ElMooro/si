"""Cycle definition separation, complete retention and failure boundaries."""
from pathlib import Path
from datetime import date, datetime, timezone, timedelta
from collections import defaultdict
from copy import deepcopy
from io import BytesIO
import ast
import csv
import gzip
import io
import json
import math
import os
import sys
import types
import unittest
import urllib.request
HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent / 'source'))
import cycle_publication as pub
import cycle_sources as sources
NOW = datetime(2026, 9, 27, 10, 30, tzinfo=timezone.utc)


class Error(Exception):
    def __init__(self, code):
        self.response = {'Error': {'Code': code}}


class Memory:
    def __init__(self):
        self.rows, self.versions, self.writes = {}, {}, []
        self.fail, self.after = None, None

    def seed(self, key, raw):
        self.rows[key] = raw
        self.versions[key] = self.versions.get(key, 0) + 1

    def get_object(self, Bucket, Key):
        if Key == self.fail:
            raise Error('AccessDenied')
        if Key not in self.rows:
            raise Error('NoSuchKey')
        return {'Body': BytesIO(self.rows[Key]), 'ContentLength': len(self.rows[Key]),
                'ETag': str(self.versions[Key]), 'LastModified': NOW-timedelta(days=1)}

    def put_object(self, Bucket, Key, Body, **kw):
        if self.fail == 'retention' and Key.startswith(pub.PRIVATE):
            raise Error('AccessDenied')
        if kw.get('IfNoneMatch') == '*' and Key in self.rows:
            raise Error('PreconditionFailed')
        if 'IfMatch' in kw and kw['IfMatch'] != str(self.versions.get(Key)):
            raise Error('PreconditionFailed')
        self.seed(Key, Body)
        self.writes.append(Key)
        if self.after:
            self.after(Key)


class Frozen(datetime):
    @classmethod
    def now(cls, tz=None):
        return NOW


def load(store):
    sources.datetime = Frozen
    path = HERE.parent / 'source/lambda_function.py'
    tree = ast.parse(path.read_text(encoding='utf-8'))
    env = {'csv': csv, 'gzip': gzip, 'io': io, 'json': json, 'math': math, 'os': os,
           'urllib': urllib, 'datetime': Frozen, 'date': date, 'timezone': timezone,
           'defaultdict': defaultdict, 'cycle_publication': pub, 'cycle_sources': sources, 'S3': store,
           'time': types.SimpleNamespace(time=lambda: 0, sleep=lambda n: None)}
    # Preserve the checked-in Eurostat country aliases. Omitting this module-level
    # update silently drops EL (Greece) and UK from otherwise complete replay.
    def alias_update(node):
        return (isinstance(node, ast.Expr) and isinstance(node.value, ast.Call)
                and isinstance(node.value.func, ast.Attribute) and node.value.func.attr == 'update'
                and isinstance(node.value.func.value, ast.Name) and node.value.func.value.id == 'ISO2_TO_3')
    nodes = [n for n in tree.body if (isinstance(n, (ast.FunctionDef, ast.ClassDef, ast.Assign)) or alias_update(n))
             and not (isinstance(n, ast.Assign) and any(isinstance(t, ast.Name) and t.id == 'S3' for t in n.targets))]
    exec(compile(ast.Module(body=nodes, type_ignores=[]), str(path), 'exec'), env)
    return env


def fixture():
    m = Memory()
    old = {'generated_at': (NOW-timedelta(days=1)).isoformat(), 'legacy': list(range(1500)),
           'countries': {'USA': {'features': {'curve': {'values': [1, 2, 3]}}}}}
    m.seed(pub.HEAD, gzip.compress(pub.encode(old), mtime=17))
    m.seed(pub.MANIFEST, pub.encode({'generated_at': old['generated_at'], 'retain': 'complete'}))
    return m, old


def packets():
    doc = {'generated_at': NOW.isoformat(), 'countries': {'USA': {'features': {}}}}
    manifest = {'generated_at': NOW.isoformat(), 'coverage': {'USA': {}}, 'features_key': pub.HEAD}
    for packet in (doc, manifest):
        packet.update({k: False for k in pub.PERMISSIONS})
    return doc, manifest


class Tests(unittest.TestCase):
    def test_whole_compiler_fixture_preserves_eurostat_greece_and_uk_aliases(self):
        env = load(Memory())
        self.assertEqual(env['ISO2_TO_3']['EL'], 'GRC')
        self.assertEqual(env['ISO2_TO_3']['UK'], 'GBR')
        months = env['month_grid']('2023-01', '2026-08')
        header = 'freq,indic,s_adj,geo\\TIME_PERIOD\t' + '\t'.join(months) + '\n'
        rows = '\n'.join('M,BS-CSMCI,SA,'+iso+'\t'+'\t'.join(['0']*len(months)) for iso in ('EL','UK'))
        env['get_bytes'] = lambda key: ((header+rows).encode(), NOW)
        feat = {iso: defaultdict(lambda: env['Series']('M')) for iso in env['ISO3']}
        env['load_eurostat'](feat, {})
        for iso in ('GRC', 'GBR'):
            self.assertEqual(feat[iso]['cons_conf_eu'].d, {month: 0 for month in months})

    def test_undated_sovereign_policy_spread_cannot_extend_or_change_oecd_curve(self):
        env = load(Memory());curve = env['Series']()
        curve.put('2026-07', 0);curve.put('2026-08', 0.5)
        data = {'generated_at': '2026-09-27T06:00:00Z', 'countries': [
            {'country': 'United States', 'yield_10y_pct': 9, 'cb_rate_pct': 1, 'as_of': 'unverified'}]}
        env['get_json'] = lambda key: data if key == 'data/global-sovereign.json' else None
        feat = {'USA': {'curve': curve}, 'KOR': {}};status, context = {}, {}
        env['load_fleet_feeds'](feat, status, context)
        self.assertEqual(curve.d, {'2026-07': 0, '2026-08': 0.5})
        self.assertEqual(context['global_sovereign']['packet'], data)
        self.assertFalse(context['global_sovereign']['monthly_observation'])
        self.assertEqual(status['global_sovereign_curve_nowcast']['countries_extended'], 0)
        self.assertFalse(status['global_sovereign_curve_nowcast']['ok'])

    def test_actual_handler_retains_all_history_fields_and_preserves_34_country_output_shape(self):
        m, old = fixture();original_head = m.rows[pub.HEAD];env = load(m)
        def oecd(feat, status):
            for iso in feat:
                curve = env['Series']()
                for month in env['month_grid']('1995-01', '2026-08'):
                    curve.put(month, 0.25)
                feat[iso]['curve'] = curve
        for key in tuple(env):
            if key.startswith('load_') and key != 'load_fleet_feeds':env[key] = lambda *args: None
        env['load_oecd_cli'] = oecd
        gs = {'countries': [{'country': name, 'yield_10y_pct': 99, 'cb_rate_pct': 0} for name in env['NAME_TO_ISO3']]}
        env['get_json'] = lambda key: gs if key == 'data/global-sovereign.json' else None
        env['lambda_handler']()
        head = pub.strict(pub.decoded(m.rows[pub.HEAD]));manifest = pub.strict(m.rows[pub.MANIFEST])
        self.assertEqual(len(head['countries']), 34)
        self.assertEqual(len(manifest['coverage']), 34)
        for country in head['countries'].values():
            row = country['features']['curve']
            self.assertEqual(row['latest_period'], '2026-08')
            self.assertIsNone(row['values'][-1])
            self.assertEqual(row['latest_value'], 0.25)
            self.assertNotIn('nowcast', row['source'])
        prior = head['publication_context']['predecessors'][pub.HEAD]
        self.assertEqual(m.rows[prior['key']], original_head)
        self.assertEqual(pub.strict(pub.decoded(m.rows[prior['key']])), old)
        snapshot = manifest['feature_snapshot']
        self.assertEqual(m.rows[snapshot['key']], m.rows[pub.HEAD])
        self.assertEqual(snapshot['decoded_sha256'], pub.sha(pub.decoded(m.rows[pub.HEAD])))
        for packet in (head, manifest):
            self.assertEqual(packet['decision']['verb'], 'WAIT')
            self.assertTrue(all(packet[k] is False for k in pub.PERMISSIONS))

    def test_invalid_dates_nonfinite_values_and_boolean_values_do_not_enter_series(self):
        env = load(Memory());month = env['to_month'];number = env['fnum'];series = env['Series']()
        for p in ('2026-13', '2026-00', '2026-02-29', '2026-Q0', '2026-Q5', '2026-1', '0000-01', 202609):
            self.assertIsNone(month(p))
            series.put(p, 1)
        for value in (True, False, None, float('nan'), float('inf'), -float('inf')):
            self.assertIsNone(number(value))
            series.put('2026-09', value)
        self.assertEqual(series.d, {})
        self.assertEqual(month('2024-02-29'), '2024-02')
        self.assertEqual(month('2026Q4'), '2026-12')
        self.assertEqual(month('2026-Q1'), '2026-03')
        self.assertEqual(number('0'), 0)
        series.put('2026-09-20', 0);series.put('2026-09-01', 999)
        self.assertEqual(series.d, {'2026-09': 0})

    def test_growth_uses_exact_months_zero_is_not_missing_and_overflow_is_withheld(self):
        env = load(Memory());series = env['Series']()
        series.put('2025-08', 100);series.put('2026-08', 120);series.put('2026-09', 130)
        self.assertAlmostEqual(series.yoy().d['2026-08'], 20)
        self.assertNotIn('2026-09', series.yoy().d)
        series.put('2025-09', 0)
        self.assertNotIn('2026-09', series.yoy().d)
        self.assertEqual(series.diff(12).d['2026-09'], 130)
        series.put('2025-07', 1e-308);series.put('2026-07', 1e308)
        self.assertNotIn('2026-07', series.pct(12).d)

    def test_missing_denied_corrupt_and_future_predecessors_do_not_reset(self):
        for mode in ('denied', 'corrupt', 'duplicate', 'future', 'nonfinite', 'retention'):
            m, _ = fixture();env = load(m);calls=[]
            env['load_oecd_cli'] = lambda *a: calls.append('provider')
            if mode == 'denied':m.fail = pub.HEAD
            if mode == 'retention':m.fail = 'retention'
            if mode == 'corrupt':m.seed(pub.HEAD, b'broken')
            if mode == 'duplicate':m.seed(pub.MANIFEST, b'{"generated_at":"2026-09-26T00:00:00Z","generated_at":"2026-09-25T00:00:00Z"}')
            if mode == 'future':m.seed(pub.MANIFEST, pub.encode({'generated_at': (NOW+timedelta(hours=1)).isoformat()}))
            if mode == 'nonfinite':m.seed(pub.HEAD, b'{"generated_at":"2026-09-26T00:00:00Z","bad":NaN}')
            with self.subTest(mode=mode), self.assertRaises(Exception):env['lambda_handler']()
            self.assertEqual(calls, [])
            self.assertFalse(any(k in (pub.HEAD, pub.MANIFEST) for k in m.writes))

    def test_concurrent_projections_never_overwrite_newer_bytes_or_roll_back(self):
        for during in (False, True):
            m, _ = fixture();state = pub.begin(m, 'b', NOW.isoformat());doc, manifest = packets()
            concurrent = pub.encode({'generated_at': (NOW+timedelta(seconds=1)).isoformat(), 'writer': 'other'})
            if during:m.after = lambda key: m.seed(pub.MANIFEST, concurrent) if key == pub.HEAD else None
            else:m.seed(pub.MANIFEST, concurrent)
            with self.assertRaises((Error, ValueError)):pub.publish(m, 'b', state, doc, manifest)
            self.assertEqual(m.rows[pub.MANIFEST], concurrent)
            self.assertEqual(pub.HEAD in m.writes, during)

    def test_wrong_authority_counts_clock_or_private_paths_cannot_publish(self):
        for mode in ('authority', 'country', 'clock', 'nonfinite'):
            m, _ = fixture();state = pub.begin(m, 'b', NOW.isoformat());doc, manifest = packets()
            if mode == 'authority':doc['sizing_eligible'] = True
            if mode == 'country':manifest['coverage'] = {}
            if mode == 'clock':manifest['generated_at'] = '2026-09-25T00:00:00Z'
            if mode == 'nonfinite':doc['invalid'] = float('nan')
            with self.assertRaises(ValueError):pub.publish(m, 'b', state, doc, manifest)
            self.assertFalse(any(k in (pub.HEAD, pub.MANIFEST) for k in m.writes))
        with self.assertRaises(ValueError):pub.read(Memory(), 'b', 'data/portfolio.json')

    def test_complete_gzip_length_empty_creation_and_retention_readback(self):
        for raw in (gzip.compress(b'{}')[:-3], gzip.compress(b'{}')+b'trailer', gzip.compress(b'{}')+gzip.compress(b'{}')):
            with self.assertRaises(ValueError):pub.decoded(raw)
        m, _ = fixture();get = m.get_object;m.get_object = lambda **kw: {**get(**kw), 'ContentLength': 0}
        with self.assertRaises(ValueError):pub.begin(m, 'b', NOW.isoformat())
        m = Memory();state = pub.begin(m, 'b', NOW.isoformat());doc, manifest = packets()
        ref = pub.publish(m, 'b', state, doc, manifest)
        self.assertEqual(pub.strict(m.rows[ref['key']])['status'], 'planned_bytes_only')
        raw = b'whole';m.seed(pub.PRIVATE+pub.sha(raw)+'.bin', b'bad')
        with self.assertRaises(ValueError):pub.retain(m, 'b', raw)

    def test_config_preserves_original_scheduler_and_runtime(self):
        conf = json.loads((HERE.parent / 'config.json').read_bytes())
        self.assertEqual(conf['memory'], 3008);self.assertEqual(conf['timeout'], 600)
        sys.path.insert(0, str(HERE.parents[3] / 'scripts'))
        from normalize_lambda_config import normalize_config
        self.assertNotIn('schedule', normalize_config(conf))


if __name__ == '__main__':
    import subprocess
    subprocess.run([sys.executable, str(HERE / 'test_acquisitions.py')], check=True)
    unittest.main(verbosity=2)
