"""Actual compiler/oracle/storage tests for the private readiness boundary."""
from pathlib import Path
from unittest.mock import patch
import json, sys, unittest
ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT/'aws/shared'), str(ROOT/'aws/ops/checks'), str(ROOT/'tests')]
import capital_structure_readiness as readiness
import capital_structure_producer as producer
import capital_structure_source as source
import capital_structure_store as store
from test_capital_structure_producer import S3
from test_capital_structure_research import fixture


def run(s3, f, request='qualification-test', clock='2026-09-25T15:00:00Z'):
    return readiness.run(s3, 'test', request, f['manifest'], f['identity'], clock=lambda: clock)


class Tests(unittest.TestCase):
    def test_full_source_replay_and_independent_oracle_advance_only_private_ready(self):
        f = fixture(); s3 = S3(f['files']); result = run(s3, f)
        self.assertTrue(result['ready_advanced'])
        ready = json.loads(s3.files[producer.READY])
        self.assertEqual(ready['counts']['provider_responses'], 7)
        self.assertEqual(ready['qualification']['original_rows_checked'], 7)
        self.assertFalse(ready['qualification']['production_measurement_formulas_imported'])
        self.assertFalse(ready['native_runtime_capacity_verified'])
        self.assertNotIn(producer.CURRENT, s3.files)
        recorded = store.verified_run(result['replay'], store.reader(s3, 'test'))
        packet = store.checked(recorded['output'], 'outputs', store.reader(s3, 'test'))
        self.assertTrue(producer.qualification_matches(ready, packet))
        writes = [v for v in s3.writes if v['Key'] == producer.READY]
        self.assertEqual(len(writes), 1); self.assertEqual(writes[0]['IfNoneMatch'], '*')
        before = len(s3.writes); self.assertEqual(run(s3, f), result); self.assertEqual(len(s3.writes), before)
        changed = dict(f); changed['manifest'] = dict(f['manifest'], sha256='0'*64)
        with self.assertRaisesRegex(ValueError, 'different originals'):run(s3, changed)

    def test_whole_predecessor_is_retained_and_ready_update_is_conditional(self):
        f = fixture(); s3 = S3(f['files'])
        old = source.encoded({'generated_at':'2026-09-24T10:00:00Z','unrecognized_payload':{'retain_everything':[1,2,3]}})
        s3.files[producer.READY] = old; etag = s3.etag(producer.READY)
        self.assertTrue(run(s3, f)['ready_advanced'])
        journal = json.loads(s3.files[producer.request_key('qualification:qualification-test')])
        self.assertEqual(s3.files[journal['previous_ready']['key']], old)
        write = next(v for v in s3.writes if v['Key'] == producer.READY)
        self.assertEqual(write['IfMatch'], etag)

    def test_incomplete_originals_stale_sources_and_failed_arithmetic_never_advance(self):
        for case in ('corrupt', 'stale', 'future', 'oracle'):
            f = fixture(); s3 = S3(f['files'])
            if case == 'corrupt':s3.files[next(iter(f['originals'].values()))['key']] += b' '
            clock = '2026-09-28T15:00:00Z' if case == 'stale' else '2026-09-24T15:00:00Z' if case == 'future' else '2026-09-25T15:00:00Z'
            original = readiness.arithmetic.verify
            with patch.object(readiness.arithmetic, 'verify', side_effect=AssertionError('arithmetic mismatch') if case == 'oracle' else original):
                with self.assertRaises((ValueError, AssertionError)):run(s3, f, clock=clock)
            self.assertNotIn(producer.READY, s3.files); self.assertNotIn(producer.CURRENT, s3.files)
            with self.assertRaisesRegex(ValueError, 'already attempted'):run(s3, f)

    def test_fresh_reader_detects_original_change_after_retention(self):
        f = fixture(); s3 = S3(f['files']); retain = store.retain
        def tamper(*args):
            reference = retain(*args); s3.files[next(iter(f['originals'].values()))['key']] += b' '
            return reference
        with patch.object(store, 'retain', side_effect=tamper):
            with self.assertRaises(ValueError):run(s3, f)
        self.assertNotIn(producer.READY, s3.files)

    def test_concurrent_ready_or_newer_source_cannot_be_overwritten(self):
        for case in ('concurrent', 'newer', 'equal'):
            f = fixture(); s3 = S3(f['files'])
            stamp = '2026-09-25T11:01:01Z' if case == 'equal' else '2026-09-25T13:00:00Z' if case == 'newer' else '2026-09-24T10:00:00Z'
            old = source.encoded({'generated_at':stamp}); s3.files[producer.READY] = old
            s3.race = case == 'concurrent'
            result = run(s3, f); self.assertFalse(result['ready_advanced'])
            self.assertEqual(s3.files[producer.READY], old)

    def test_budget_expiry_after_replay_cannot_advance_ready(self):
        f = fixture(); s3 = S3(f['files']); replay = store.replay; tick = [0]
        def expire(*args):
            result = replay(*args); tick[0] = 9999; return result
        with patch.object(readiness.time, 'monotonic', side_effect=lambda:tick[0]), patch.object(store, 'replay', side_effect=expire):
            with self.assertRaises(TimeoutError):run(s3, f)
        self.assertNotIn(producer.READY, s3.files)
        for budget in (True, float('nan'), float('inf'), 0, 599, 5101):
            with self.assertRaises(ValueError):readiness.run(s3, 'test', 'other', f['manifest'], f['identity'], budget)


if __name__ == '__main__':unittest.main(verbosity=2)
