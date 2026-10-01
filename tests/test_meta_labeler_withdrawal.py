"""Actual predecessor replay with invented inputs: bug proof, not performance."""
import ast
import copy
import gzip
import hashlib
import io
import json
import math
from pathlib import Path
import sys
import time
import types
import unittest
from datetime import datetime, timedelta, timezone

ROOT = Path(__file__).resolve().parents[1]
FIX = ROOT / 'tests/fixtures/meta-labeler'
sys.path.insert(0, str(ROOT / 'aws/shared'))
from meta_labeler_authority import blocked_packet, meta_context


def scope(path):
    tree = ast.parse(path.read_text(encoding='utf-8'))
    nodes = []
    for n in tree.body:
        if isinstance(n, ast.FunctionDef):
            nodes.append(n)
        elif isinstance(n, ast.Assign):
            try:
                ast.literal_eval(n.value)
            except (TypeError, ValueError):
                continue
            nodes.append(n)
    env = dict(json=json, gzip=gzip, math=math, datetime=datetime,
               timezone=timezone, time=time, blocked_packet=blocked_packet,
               meta_context=meta_context)
    exec(compile(ast.Module(body=nodes, type_ignores=[]), str(path), 'exec'), env)
    return env


class MemoryS3:
    def __init__(self, packet=None):
        self.packet, self.reads, self.writes = packet, [], {}
    def get_object(self, **kw):
        self.reads.append(kw['Key'])
        return {'Body': io.BytesIO(gzip.compress(json.dumps(self.packet).encode()))}
    def put_object(self, **kw):
        self.writes[kw['Key']] = json.loads(kw['Body'])


def legacy(rows):
    env = scope(FIX / 'legacy-writer.py.txt')
    s3 = MemoryS3(rows)
    env.update(S3=s3, DDB=types.SimpleNamespace(Table=lambda name: types.SimpleNamespace(
        scan=lambda **kw: {'Items': []})), spy_context=lambda: lambda d: {
            'mom21': .01, 'vol20': .02, 'above50': 1.})
    captured = {}
    def train(X, Y, **kw):
        captured.update(X=copy.deepcopy(X), Y=Y[:])
        return [0.] * len(X[0])
    env['train_logistic'] = train
    env['lambda_handler']()
    return captured, s3.writes['data/meta-labeler.json']


class MetaWithdrawalTests(unittest.TestCase):
    def test_predecessor_exact_bytes(self):
        manifest = json.loads((FIX / 'manifest.json').read_text(encoding='utf-8'))
        self.assertEqual(hashlib.sha256((FIX / 'legacy-writer.py.txt').read_bytes()).hexdigest(), manifest['sha256'])

    def test_same_date_and_future_label_enter_baseline_training(self):
        # All dates identical: row 6 trains and row 7 tests at the same instant.
        # Extra availability fields are deliberately ignored by the real writer.
        rows = [dict(date='2026-01-01', type='a', conf=.5, dir=1, ex=.1,
                     label_end_at='2026-02-01T21:00:00Z',
                     label_available_at='2026-02-02T00:00:00Z') for _ in range(10)]
        a, out = legacy(rows)
        rows[6]['ex'] = -.1  # Outcome not available on test signal date.
        b, _ = legacy(rows)
        self.assertEqual(len(a['Y']), 7)
        self.assertNotEqual(a['Y'], b['Y'])
        self.assertEqual(out['model']['n_test'], 3)

    def test_future_types_change_baseline_training_feature_selection(self):
        rows = [dict(date=f'2026-01-{i+1:02}', type=t, conf=.5, dir=1, ex=.1)
                for i, t in enumerate(['a','b','c','d','e','a','b','a','a','a'])]
        a, out_a = legacy(rows)
        for row in rows[7:]: row['type'] = 'future_only'
        b, out_b = legacy(rows)
        self.assertNotEqual(list(out_a['model']['coefficients']), list(out_b['model']['coefficients']))
        self.assertNotEqual(a['X'], b['X'])

    def test_same_day_close_changes_baseline_feature(self):
        env = scope(FIX / 'legacy-writer.py.txt')
        prices = [{'date': (datetime(2026,1,1)+timedelta(days=i)).date().isoformat(),
                   'price': 100.} for i in range(60)]
        env.update(FMP_KEY='unused', jget=lambda url: prices)
        a = env['spy_context']()(prices[-1]['date'])
        prices[-1]['price'] = 150.
        b = env['spy_context']()(prices[-1]['date'])
        self.assertNotEqual(a, b)

    def test_writer_never_reads_sources_or_fits_even_with_override(self):
        env = scope(ROOT / 'aws/lambdas/justhodl-meta-labeler/source/lambda_function.py')
        s3 = MemoryS3()
        env['S3'] = s3
        for event in (None, {}, {'force': True, 'status': 'active', 'validated_strategy': True}):
            result = env['lambda_handler'](event)
            out = s3.writes['data/meta-labeler.json']
            self.assertEqual(result['statusCode'], 200)
            self.assertEqual(out['qualification_status'], 'BLOCKED')
            self.assertFalse(out['decision_eligible'])
            self.assertTrue(all(v is None for v in out['model'].values()))
            self.assertEqual(out['gates'], [])
            self.assertIsNone(out['n_take'])
            self.assertIsNone(out['n_training_rows'])
        self.assertEqual(s3.reads, [])
        self.assertEqual(set(s3.writes), {'data/meta-labeler.json'})

    def test_projection_rejects_every_authority_claim_and_is_immutable(self):
        for packet in (None, [], True, 'TAKE', {}, {'contract': 'meta-labeler-withdrawal.v1',
                'status': 'active', 'decision_eligible': True, 'generated_at': '2999-01-01',
                'model': {'uplift_pp': 98765}, 'gates': [{'verdict': 'TAKE'}],
                'methodology': 'validated strategy'}):
            before = copy.deepcopy(packet)
            out = meta_context(packet)
            self.assertEqual(out, blocked_packet())
            self.assertNotIn('98765', json.dumps(out))
            self.assertEqual(before, packet)
        a = blocked_packet(); a['gates'].append('TAKE')
        self.assertEqual(blocked_packet()['gates'], [])

    def test_ask_desk_actual_fetch_masks_stale_packet(self):
        env = scope(ROOT / 'aws/lambdas/justhodl-ask-desk/source/lambda_function.py')
        class S3:
            def get_object(self, **kw):
                return {'Body': io.BytesIO(json.dumps({'gates':[{'verdict':'TAKE'}],
                    'model':{'uplift_pp':98765}, 'status':'active'}).encode())}
        env['S3'] = S3()
        out = json.loads(env['fetch_slim']('data/meta-labeler.json'))
        self.assertEqual(out['qualification_status'], 'BLOCKED')
        self.assertFalse(out['decision_eligible'])
        self.assertEqual(out['gates'], [])
        self.assertNotIn('98765', json.dumps(out))

if __name__ == '__main__': unittest.main()
