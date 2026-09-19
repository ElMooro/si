"""Exercise the actual board normalizer and aggregation denominator."""
from datetime import datetime, timezone, timedelta
from pathlib import Path
import ast
import json
import time
import unittest

ROOT = Path(__file__).resolve().parents[4]


def functions(names, scope):
    path = ROOT / 'aws/lambdas/justhodl-signal-board/source/lambda_function.py'
    nodes = [v for v in ast.parse(path.read_text(encoding='utf-8')).body
             if isinstance(v, ast.FunctionDef) and v.name in names]
    if len(nodes) != len(names):
        raise AssertionError('Actual board function missing')
    exec(compile(ast.Module(body=nodes, type_ignores=[]), str(path), 'exec'), scope)
    return scope


class Tests(unittest.TestCase):
    def test_legacy_scores_and_current_descriptive_source_do_not_vote(self):
        fn = functions({'n_globalflows'}, {})['n_globalflows']
        for packet in ({}, {'inst_vs_retail': {'institutional': 99, 'retail': 99}, 'hot_money': {'n_scored': 20}},
                       {'contract': 'global-flow-research.v1', 'quality': {'aligned_funds': 81, 'configured_funds': 128}}):
            self.assertIsNone(fn(packet)[0])

    def test_research_does_not_dilute_an_independent_signal(self):
        class Storage:
            def __init__(self): self.objects = {}
            def put_object(self, **kw): self.objects[kw['Key']] = json.loads(kw['Body'])
            def get_object(self, **kw): raise KeyError('No previous posture; no event emission')
        client = Storage(); stamp = datetime.now(timezone.utc)
        packet = {'contract': 'global-flow-research.v1', 'generated_at': stamp.isoformat(),
                  'calls_eligible': False, 'quality': {'aligned_funds': 81, 'configured_funds': 128}}
        scope = functions({'lambda_handler', 'n_globalflows', 'clamp'}, {
            'time': time, 'datetime': datetime, 'timezone': timezone, 'timedelta': timedelta,
            'json': json, 's3': client, 'S3_BUCKET': 'fixture', 'OUT_KEY': 'data/signal-board.json',
            'STALE_HOURS': 48, 'SIG_LABEL': {1: 'POSITIVE'}, 'guard_output': None,
            'read_json': lambda key: (packet, stamp)})
        scope['FEEDS'] = [('Global Flows', 'flow', 'fixture', scope['n_globalflows']),
                          ('Independent test vote', 'macro', 'fixture', lambda d: (1, 'fixture'))]
        self.assertEqual(scope['lambda_handler']({}, None)['statusCode'], 200)
        output = client.objects['data/signal-board.json']
        self.assertEqual(output['n_live'], 1)
        self.assertEqual(output['composite_signal'], 1)
        self.assertIsNone(output['engines'][0]['signal'])
