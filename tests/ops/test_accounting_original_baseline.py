from pathlib import Path
from datetime import datetime, timezone
from io import BytesIO
from unittest.mock import Mock
import sys
import unittest

ROOT = Path(__file__).resolve().parents[2]
sys.path[:0] = [str(ROOT / p) for p in ('aws/ops/staged', 'aws/ops', 'aws/ops/checks')]
import ops_6233_accounting_original_baseline as op


class Error(Exception):
    def __init__(self, code):
        self.response = {'Error': {'Code': code}}


class Memory:
    def __init__(self):
        self.rows = {}; self.reads = []; self.writes = []; self.truncate = False

    def get_object(self, **kw):
        key = kw['Key']; self.reads.append(key)
        if key not in self.rows:
            raise Error('NoSuchKey')
        raw = self.rows[key]
        return {'Body': BytesIO(raw), 'ContentLength': len(raw) + int(self.truncate), 'ETag': 'version',
                'LastModified': datetime(2026, 9, 27, tzinfo=timezone.utc)}

    def put_object(self, **kw):
        assert kw['Key'].startswith(op.PRIVATE) and kw['IfNoneMatch'] == '*'
        if kw['Key'] in self.rows:
            raise Error('PreconditionFailed')
        self.rows[kw['Key']] = kw['Body']; self.writes.append(kw['Key'])


class Tests(unittest.TestCase):
    def test_all_whole_sources_are_pinned_without_importing_producers(self):
        for fn, (digest, _) in op.SOURCES.items():
            raw = (ROOT / 'tests/fixtures' / ('pre-' + fn.removeprefix('justhodl-') + '-accounting-research.py.txt')).read_bytes()
            self.assertEqual(op.source_check(fn, raw)['sha256'], digest)
            with self.assertRaises(ValueError): op.source_check(fn, raw + b'\n')
        self.assertNotIn('lambda_function', sys.modules)

    def test_foreign_and_private_objects_are_rejected_before_any_read(self):
        memory = Memory()
        for key in ('data/pm-decision.json', 'learning/morning_run_log.json', 'private/account.json', 'data/other-filings.json'):
            with self.assertRaises(ValueError): op.capture(memory, key)
        self.assertEqual(memory.reads, [])

    def test_every_original_byte_is_retained_even_if_malformed_or_empty(self):
        memory = Memory()
        for key, raw in zip(op.KEYS, (b'{"zero":0,"missing":null}', b'{malformed', b'')):
            memory.rows[key] = raw; result = op.capture(memory, key)
            self.assertEqual(memory.rows[result['original']['key']], raw)
            self.assertEqual(memory.rows[key], raw)
            self.assertFalse(result['original_provider_verified'])
            self.assertEqual(op.capture(memory, key), result)
        self.assertEqual(len(memory.writes), 3)

    def test_missing_denied_truncated_and_corrupt_states_remain_distinct(self):
        self.assertEqual(op.capture(Memory(), op.KEYS[0]), {'status': 'missing'})
        s3 = Mock(); s3.get_object.side_effect = Error('AccessDenied')
        with self.assertRaises(Error): op.capture(s3, op.KEYS[0])
        memory = Memory(); memory.rows[op.KEYS[0]] = b'{}'; memory.truncate = True
        with self.assertRaises(ValueError): op.capture(memory, op.KEYS[0])
        self.assertEqual(memory.writes, [])
        memory = Memory(); memory.rows[op.PRIVATE + op.sha(b'whole') + '.bin'] = b'corrupt'
        with self.assertRaises(ValueError): op.retain(memory, b'whole')


if __name__ == '__main__':
    unittest.main()
