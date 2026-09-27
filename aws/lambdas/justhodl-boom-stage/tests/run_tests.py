"""Exercise actual history writer and complete native handler without network/AWS."""
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from io import BytesIO
from pathlib import Path
from unittest.mock import patch
import ast
import hashlib
import importlib.util
import json
import sys
import types
import unittest

ROOT = Path(__file__).resolve().parents[4]
SOURCE = Path(__file__).resolve().parents[1]/'source'
sys.path.insert(0, str(SOURCE))
import boom_history as h
NOW = datetime(2026, 9, 27, 12, 30, tzinfo=timezone.utc)


class Error(Exception):
    def __init__(self, code):
        self.response = {'Error': {'Code': code}}


class Memory:
    def __init__(self, days=120):
        history = {'unknown_original': {'zero': 0, 'whole': [None, 'preserve']}, 'days': {
            (NOW.date()-timedelta(days=i)).isoformat(): {'KR-semis': {'v': 0, 'vol': None, 'stage': 'NA'}}
            for i in range(1, days+1)}}
        self.data = {h.HISTORY: h.encode(history), h.HEAD: h.encode({'generated_at': (NOW-timedelta(days=1)).isoformat(), 'legacy': True})}
        self.public_writes = []
        self.reads = []
        self.before_public = None
        self.fail_private = False
        self.denied = set()
    def get_object(self, Bucket, Key):
        self.reads.append(Key)
        if Key in self.denied:
            raise Error('AccessDenied')
        if Key not in self.data:
            raise Error('NoSuchKey')
        raw = self.data[Key]
        return {'Body': BytesIO(raw), 'ContentLength': len(raw), 'ETag': h.sha(raw)}
    def put_object(self, Bucket, Key, Body, **kw):
        private = Key.startswith(h.PRIVATE)
        if private and self.fail_private:
            raise Error('AccessDenied')
        if not private and self.before_public:
            self.before_public(Key)
        previous = self.data.get(Key)
        if (kw.get('IfNoneMatch') == '*' and previous is not None
                or 'IfMatch' in kw and (previous is None or kw['IfMatch'] != h.sha(previous))):
            raise Error('PreconditionFailed')
        self.data[Key] = Body
        if not private:
            self.public_writes.append(Key)


def pair(ident='KR-semis'):
    return {'id': ident, 'stage': 'NA', 'value': {'yoy_pct': 0}, 'volume': {'vs_baseline_pct': None}}


def packet():
    return {'generated_at': NOW.isoformat(), 'pairs': [pair()], 'unknown': [0, None]}


def native(memory):
    boto = types.ModuleType('boto3'); boto.client = lambda *a, **kw: memory
    secret = types.ModuleType('managed_secret'); secret.managed_secret = lambda *a, **kw: ''
    spec = importlib.util.spec_from_file_location('boom_native_fixture', SOURCE/'lambda_function.py')
    mod = importlib.util.module_from_spec(spec)
    with patch.dict(sys.modules, {'boto3': boto, 'managed_secret': secret}):
        spec.loader.exec_module(mod)
    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return NOW.astimezone(tz) if tz else NOW.replace(tzinfo=None)
    mod.datetime = Clock
    mod._yoy_yahoo = lambda *a: None
    mod._factor4 = lambda *a: None
    return mod


def publish(ledger, packet):
    return native(ledger.client)._publish_plan(ledger.prepare(packet), ledger)


class Tests(unittest.TestCase):
    def test_actual_native_keeps_all_120_dates_and_all_20_pairs(self):
        mem = Memory(); original = deepcopy(mem.data); mod = native(mem)
        with patch('urllib.request.urlopen', side_effect=AssertionError('No provider calls in fixture')):
            mod.lambda_handler()
        hist = h.decode(mem.data[h.HISTORY]); out = h.decode(mem.data[h.HEAD])
        self.assertEqual(len(hist['days']), 121)
        self.assertEqual(len(hist['days']['2026-09-27']), 20)
        self.assertEqual(len(out['pairs']), 20)
        for day, rows in h.decode(original[h.HISTORY])['days'].items():
            self.assertEqual(hist['days'][day], rows)
        self.assertEqual(hist['unknown_original'], h.decode(original[h.HISTORY])['unknown_original'])
        self.assertEqual(mem.public_writes, [h.HISTORY, h.HEAD])
        self.assertTrue(all(out[key] is False for key in h.FLAGS))
        evidence = out['history_preservation']
        self.assertFalse(evidence['provider_originals_replayed'])
        for key, ref in ((h.HISTORY, evidence['predecessor_history']), (h.HEAD, evidence['predecessor_head'])):
            self.assertEqual(mem.data[ref['key']], original[key])
        calculation = h.decode(mem.data[evidence['complete_native_calculation']['key']])
        self.assertEqual(calculation['pairs'], out['pairs'])
        self.assertEqual(len(evidence['compiler_sha256']), 3)

    def test_denied_malformed_nonfinite_duplicate_and_future_predecessors_cannot_reset_history(self):
        for raw in (b'', b'bad', b'[]', b'{"days":{}} trailing', b'{"days":{},"days":{}}',
                    b'{"days":{},"value":1e400}', h.encode({'days': {'2026-09-28': {}}}),
                    h.encode({'days': {'2026-02-30': {}}}), h.encode({'days': {'2026-09-25': None}})):
            mem = Memory(); mem.data[h.HISTORY] = raw; original = deepcopy(mem.data)
            with self.assertRaises(h.IntegrityError): h.Ledger(mem, 'bucket', NOW.isoformat())
            self.assertEqual(mem.public_writes, [])
            self.assertEqual(mem.data[h.HISTORY], original[h.HISTORY])
            self.assertIn(h.PRIVATE+h.sha(raw)+'.bin', mem.data)
        mem = Memory(); mem.denied.add(h.HISTORY)
        with self.assertRaises(h.IntegrityError): h.Ledger(mem, 'bucket', NOW.isoformat())
        self.assertEqual(mem.public_writes, [])

    def test_missing_history_initializes_only_once_and_same_day_never_overwrites(self):
        mem = Memory(); del mem.data[h.HISTORY]
        ledger = h.Ledger(mem, 'bucket', NOW.isoformat()); ledger.append([pair()]); publish(ledger, packet())
        self.assertEqual(len(h.decode(mem.data[h.HISTORY])['days']), 1)
        with self.assertRaises(h.IntegrityError): h.Ledger(mem, 'bucket', NOW.isoformat())
        later = h.Ledger(mem, 'bucket', (NOW+timedelta(minutes=1)).isoformat())
        with self.assertRaises(h.IntegrityError): later.append([pair()])

    def test_old_date_deletion_and_duplicate_pairs_fail_before_publication(self):
        mem = Memory(); ledger = h.Ledger(mem, 'bucket', NOW.isoformat())
        with self.assertRaises(h.IntegrityError): ledger.append([pair(), pair()])
        ledger.append([pair()]); ledger.history['days'].pop(next(iter(ledger.prior_days)))
        with self.assertRaises(h.IntegrityError): publish(ledger, packet())
        self.assertEqual(mem.public_writes, [])

    def test_retention_denial_and_read_truncation_do_not_mutate_public_state(self):
        mem = Memory(); ledger = h.Ledger(mem, 'bucket', NOW.isoformat()); ledger.append([pair()])
        mem.fail_private = True
        with self.assertRaises(h.IntegrityError): publish(ledger, packet())
        self.assertEqual(mem.public_writes, [])

    def test_output_population_and_original_unknown_metadata_cannot_drift(self):
        for change in ('population', 'number', 'metadata'):
            mem = Memory(); ledger = h.Ledger(mem, 'bucket', NOW.isoformat()); ledger.append([pair()])
            out = packet()
            if change == 'population': out['pairs'] = []
            if change == 'number': out['pairs'][0]['value']['yoy_pct'] = 10
            if change == 'metadata': ledger.history['unknown_original']['zero'] = 1
            with self.assertRaises(h.IntegrityError): publish(ledger, out)
            self.assertEqual(mem.public_writes, [])
        mem = Memory(); original = mem.get_object
        mem.get_object = lambda **kw: {**original(**kw), 'ContentLength': 999999}
        with self.assertRaises(h.IntegrityError): h.Ledger(mem, 'bucket', NOW.isoformat())
        self.assertEqual(mem.public_writes, [])

    def test_concurrent_changes_before_checks_and_at_each_write_are_preserved(self):
        for key in (h.HISTORY, h.HEAD):
            mem = Memory(); ledger = h.Ledger(mem, 'bucket', NOW.isoformat()); ledger.append([pair()])
            mem.data[key] = b'foreign original'
            with self.assertRaises(h.IntegrityError): publish(ledger, packet())
            self.assertEqual(mem.data[key], b'foreign original'); self.assertEqual(mem.public_writes, [])
        for key in (h.HISTORY, h.HEAD):
            mem = Memory(); old_head = mem.data[h.HEAD]
            ledger = h.Ledger(mem, 'bucket', NOW.isoformat()); ledger.append([pair()])
            def competing(actual):
                if actual == key: mem.data[key] = b'foreign writer'
            mem.before_public = competing
            with self.assertRaises(Error): publish(ledger, packet())
            self.assertEqual(mem.data[key], b'foreign writer')
            self.assertEqual(mem.public_writes, [] if key == h.HISTORY else [h.HISTORY])
            if key == h.HISTORY: self.assertEqual(mem.data[h.HEAD], old_head)
            self.assertTrue(any(b'planned_bytes_only' in v for k, v in mem.data.items() if k.startswith(h.PRIVATE)))

    def test_whole_predecessor_and_other_functions_are_preserved(self):
        old = (ROOT/'tests/fixtures/pre-shipping-qualification-boom-stage.py.txt').read_bytes()
        self.assertEqual(hashlib.sha256(old).hexdigest(), 'e44679e4aa09eacd2e8702d31d0aca845c3587533a6046286e5bc6730396f055')
        functions = lambda raw: {n.name: ast.dump(n) for n in ast.parse(raw).body if isinstance(n, (ast.FunctionDef, ast.ClassDef))}
        before, after = functions(old), functions((SOURCE/'lambda_function.py').read_bytes())
        for name in before:
            if name != 'lambda_handler': self.assertEqual(before[name], after[name], name)


if __name__ == '__main__':
    unittest.main(verbosity=2)
