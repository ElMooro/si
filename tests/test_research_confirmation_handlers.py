"""Actual handler + shared reader boundaries with wholly invented inputs only."""
from pathlib import Path
from datetime import datetime, timezone
from unittest.mock import patch
import ast
import contextlib
import copy
import hashlib
import importlib.util
import io
import json
import sys
import types
import unittest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'tests'))
from test_confirmation_loader import healthy, KEYS, load, deny, ReadFailure

ARCHIVE = ROOT / 'tests/fixtures/confirmation-reader'
MANIFEST = json.loads((ARCHIVE / 'predecessors.json').read_bytes())
POLICY = json.loads((ROOT / 'tests/fixtures/no-paid-research/preservation.json').read_bytes())
NOW = datetime(2026, 10, 1, 12, tzinfo=timezone.utc)


class FixedDate(datetime):
    @classmethod
    def now(cls, tz=None):
        return NOW


class MemoryS3:
    def __init__(self, documents, output):
        self.documents = documents
        self.output = output
        self.reads = []
        self.writes = []
        self.confirmation_streams = []

    def get_object(self, **kw):
        assert kw['Bucket'] == 'justhodl-dashboard-live'
        key = kw['Key']
        assert key in self.documents, 'Unexpected data access: ' + key
        self.reads.append(key)
        value = self.documents[key]
        if isinstance(value, Exception):
            raise value
        raw = value if isinstance(value, bytes) else json.dumps(value).encode()
        stream = io.BytesIO(raw)
        if key in KEYS:
            self.confirmation_streams.append(stream)
        return {'Body': stream, 'ContentLength': len(raw)}

    def put_object(self, **kw):
        assert kw['Bucket'] == 'justhodl-dashboard-live' and kw['Key'] == self.output
        self.writes.append(json.loads(kw['Body']))
        return {}


def run_handler(name, feeds=None, predecessor=False, cold=False):
    src = 'data/compound-signals.json' if name == 'alpha' else 'data/opportunities.json'
    dest = 'data/alpha-scoreboard-research.json' if name == 'alpha' else 'data/opportunities-research.json'
    documents = healthy() if feeds is None else copy.deepcopy(feeds)
    documents[src] = ({'compound': [{'symbol': 'TEST', 'compound_score': 0, 'n_systems': 0, 'systems': []}]}
                      if name == 'alpha' else {'all': [{'ticker': 'TEST', 'verdict': 'OPPORTUNITY', 'opportunity_score': 0}]})
    documents[dest] = {} if cold else {'by_ticker': {'TEST': {
        'thesis': 'Invented cached prose.', 'bear': 'Invented risk.',
        'thesis_ver': 'alpha-1' if name == 'alpha' else 'opp-1', 'thesis_at': NOW.isoformat()}}}
    client = MemoryS3(documents, dest)
    ee = load(client, ARCHIVE / 'equity_enrich.py.txt' if predecessor else None)
    ee.fetch_peer_pe = lambda: ({}, {})
    ee.fetch_financials = lambda ticker: {'price': 100, 'financials': []}
    ee.grade_track_record = lambda *a: {'status': 'invented_test_only'}
    thesis_calls = []

    def invented_thesis(*args):
        thesis_calls.append(args)
        return 'Invented generated prose.', 'Invented risk.'

    if predecessor:
        ee.make_thesis = invented_thesis if cold else deny
    fake = {'boto3': types.SimpleNamespace(client=lambda *a, **kw: client, resource=deny),
            'equity_enrich': ee, 'short_position_context': ee._fixture_context}
    path = ARCHIVE / (name + '.py.txt') if predecessor else ROOT / f'aws/lambdas/justhodl-{name}-research/source/lambda_function.py'
    # The .txt archive is intentionally inert; load its complete reviewed bytes.
    module = types.ModuleType('whole_' + name)
    with patch.dict(sys.modules, fake), patch('urllib.request.urlopen', deny), patch('socket.create_connection', deny):
        exec(compile(path.read_bytes(), str(path), 'exec'), module.__dict__)
        module.datetime = FixedDate
        module.time = types.SimpleNamespace(time=lambda: 100.0)
        if name == 'alpha':
            module.compute_changes = lambda *a: {'first_run': True}
            module.log_signals = lambda *a: 0
        with contextlib.redirect_stdout(io.StringIO()):
            response = module.lambda_handler({}, None)
    assert response['statusCode'] == 200 and len(client.writes) == 1
    assert client.reads == [src, dest, *KEYS]
    if not predecessor:
        assert all(s.closed for s in client.confirmation_streams)
    json.dumps(client.writes[0], allow_nan=False)
    return client.writes[0], thesis_calls


class WholeHandlers(unittest.TestCase):
    def test_preserved_whole_source_and_no_unrelated_handler_changes(self):
        for source, entry in MANIFEST.items():
            raw = (ROOT / entry['file']).read_bytes()
            self.assertEqual(len(raw), entry['bytes'])
            self.assertEqual(hashlib.sha256(raw).hexdigest(), entry['sha256'])
            current = (ROOT / source).read_text(encoding='utf-8')
            # Reverse only the separately tested no-paid policy change before
            # applying the original confirmation-reader preservation assertions.
            for before, after in reversed(POLICY['edits'].get(source, [])):
                self.assertEqual(current.count(after), 1)
                current = current.replace(after, before)
            original = raw.decode('utf-8')
            if source.endswith('equity_enrich.py'):
                def unrelated(text):
                    nodes = [n for n in ast.parse(text).body if not (
                        isinstance(n, ast.FunctionDef) and n.name in ('load_confirmation_feeds', '_confirmation_document')
                        or isinstance(n, ast.Assign) and any(isinstance(v, ast.Name) and v.id == 'CONFIRMATION_BODY_LIMIT' for v in n.targets))]
                    return ast.dump(ast.Module(body=nodes, type_ignores=[]), include_attributes=False)
                self.assertEqual(unrelated(current), unrelated(original))
            else:
                current = current.replace('si_f, f13_f, fwd_f, chain_f, confirmation_feeds = EE.load_confirmation_feeds(\n        with_availability=True, descriptive_short_positions=True)', 'si_f, f13_f, fwd_f, chain_f = EE.load_confirmation_feeds()')
                current = current.replace('        rec["short_position_context"] = s or None\n', '')
                current = current.replace('        "confirmation_feeds": confirmation_feeds,\n', '')
                self.assertEqual(ast.dump(ast.parse(current)), ast.dump(ast.parse(original)))

    def test_all_previous_healthy_published_values_preserved(self):
        for name in ('alpha', 'opportunities'):
            old, _ = run_handler(name, predecessor=True)
            new, _ = run_handler(name)
            new.pop('confirmation_feeds')
            new['by_ticker']['TEST'].pop('short_position_context')
            self.assertEqual(new.pop('narrative')['status'], 'unqualified')
            self.assertEqual(new.pop('narrative_statuses'), 1)
            self.assertEqual(new.pop('version'), '1.1.0')
            self.assertEqual(old.pop('version'), '1.0.0')
            for key in ('thesis', 'bear', 'thesis_at', 'thesis_ver'):
                new['by_ticker']['TEST'].pop(key)
                old['by_ticker']['TEST'].pop(key)
            self.assertEqual(new, old, name)

    def test_bad_feed_cannot_abort_remaining_reads_or_publication(self):
        for name in ('alpha', 'opportunities'):
            for key in KEYS:
                feeds = healthy(); feeds[key] = [1, 'bad']
                output, _ = run_handler(name, feeds)
                self.assertEqual(output['n'], 1)
                self.assertEqual(sum(m['read_status'] == 'parsed' for m in output['confirmation_feeds'].values()), 3)

    def test_reconciled_ratio_and_source_pointer_reach_real_publication(self):
        for name in ('alpha', 'opportunities'):
            feeds = healthy()
            feeds[KEYS[0]]['by_ticker']['TEST'].update(days_to_cover='999', dtc_effective='2', dtc_reconstructed='2', dtc_status='provider_differs_from_reconstructed_ratio')
            output, _ = run_handler(name, feeds)
            row = output['by_ticker']['TEST']['short_position_context']
            self.assertEqual(row['days_to_cover'], 2)
            self.assertEqual(row['reported_days_to_cover'], 999)
            self.assertEqual(row['source_row'], '/by_ticker/TEST')
            self.assertIsNone(output['by_ticker']['TEST']['short_pct'])
            self.assertIsNone(output['by_ticker']['TEST']['short_signal'])
            self.assertEqual(output['confirmation_feeds']['short_interest']['body_sha256'], hashlib.sha256(json.dumps(feeds[KEYS[0]]).encode()).hexdigest())

    def test_ambiguous_or_foreign_contract_rows_cannot_enter_ticker_context(self):
        for name in ('alpha', 'opportunities'):
            feeds = healthy(); feeds[KEYS[0]]['by_ticker']['test'] = {'short_interest': 999}
            output, _ = run_handler(name, feeds)
            self.assertIsNone(output['by_ticker']['TEST']['short_position_context'])
            self.assertEqual(output['confirmation_feeds']['short_interest']['ambiguous_ticker_count'], 1)
            occurrences = output['confirmation_feeds']['short_interest']['ambiguous_symbols'][0]['occurrences']
            self.assertEqual([r['source_row'] for r in occurrences], ['/by_ticker/TEST', '/by_ticker/test'])
            feeds[KEYS[0]]['contract'] = 'unrecognized'
            output, _ = run_handler(name, feeds)
            self.assertIsNone(output['by_ticker']['TEST']['short_position_context'])
            self.assertEqual(output['confirmation_feeds']['short_interest']['context_status'], 'unavailable')

    def test_denied_feed_is_unavailable_without_error_or_private_text(self):
        for name in ('alpha', 'opportunities'):
            feeds = healthy(); feeds[KEYS[0]] = ReadFailure({'Error': {'Code': 'AccessDenied', 'Message': 'PRIVATE_CANARY'}})
            output, _ = run_handler(name, feeds)
            self.assertIsNone(output['by_ticker']['TEST']['short_position_context'])
            self.assertEqual(output['confirmation_feeds']['short_interest']['read_status'], 'unavailable')
            self.assertNotIn('PRIVATE_CANARY', json.dumps(output))

    def test_zero_and_historical_report_are_descriptive_not_signal(self):
        for name in ('alpha', 'opportunities'):
            feeds = healthy(); feeds[KEYS[0]]['by_ticker']['TEST'].update(latest=False, settlement_date='2000-01-01', calls_eligible=True, score=99)
            output, _ = run_handler(name, feeds)
            row = output['by_ticker']['TEST']['short_position_context']
            self.assertEqual(row['short_interest_shares'], 0)
            self.assertFalse(row['latest_reported'])
            self.assertFalse(row['observation_freshness_verified'])
            for key in ('calls_eligible', 'sizing_eligible', 'execution_eligible', 'forecast_qualified'):
                self.assertFalse(row[key])
            self.assertIsNone(row['score'])

    def test_context_is_not_sent_to_a_model_and_old_prompt_path_is_reproduced(self):
        for name in ('alpha', 'opportunities'):
            old, old_calls = run_handler(name, predecessor=True, cold=True)
            new, new_calls = run_handler(name, cold=True)
            self.assertEqual(len(old_calls), 1)
            self.assertEqual(new_calls, [])
            self.assertEqual(new['new_theses'], 0)
            self.assertEqual(new['narrative']['model_requests_enabled'], False)


if __name__ == '__main__':
    unittest.main(verbosity=2)
