"""Actual snapshot handlers interleaved at S3 reads; all services in memory."""
import importlib.util
import json
from pathlib import Path
import types
from datetime import datetime, timezone
HERE = Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location('integration_suite', HERE.parents[1] / 'justhodl-backtest-engine/tests/run_tests.py')
suite = importlib.util.module_from_spec(spec)
spec.loader.exec_module(suite)


def producer(s3, hour):
    spec = importlib.util.spec_from_file_location('snapshotter_' + str(hour), HERE.parent / 'source/lambda_function.py')
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    mod.S3 = s3
    mod.safe_get_ssm = lambda name: {'sig': 1.3} if name.endswith('weights') else {}
    mod.count_outcomes_60d = lambda: ({}, 0)
    mod.datetime = types.SimpleNamespace(now=lambda tz: datetime(2026, 9, 6, hour, 0, tzinfo=timezone.utc), fromisoformat=datetime.fromisoformat)
    return mod


def test_actual_concurrent_handlers_preserve_versions_and_newest_model():
    s3 = suite.MemoryS3()
    report = b'{"calibrator_report":"keep-horizon-schema"}'
    s3.objects['calibration/latest.json'] = report
    producer(s3, 10).lambda_handler()
    earlier, later = producer(s3, 12), producer(s3, 18)
    raced = []
    def interrupt(key):
        if key == 'calibration/index.json' and not raced:
            raced.append(True)
            # The first handler already read its old ETag/body. The other
            # actual handler completes before the first attempts its PUT.
            later.lambda_handler()
    s3.on_read = interrupt
    earlier.lambda_handler()
    assert raced and s3.conflicts >= 1, 'must exercise a real stale-ETag write'
    idx = json.loads(s3.objects['calibration/index.json'])
    assert len(idx['versions']) == 3
    assert len({r['snapshot_id'] for r in idx['versions']}) == 3
    latest = json.loads(s3.objects['calibration/model-latest.json'])
    assert latest['available_at'].startswith('2026-09-06T18:')
    weekly = json.loads(s3.objects['calibration/history-index.json'])['snapshots']
    assert len(weekly) == 1 and weekly[0]['key'].startswith('calibration/versions/')
    assert weekly[0]['snapshot_id'] == latest['snapshot_id']
    assert s3.objects['calibration/latest.json'] == report


def test_unindexed_version_is_recovered_without_destroying_read_failure():
    s3 = suite.MemoryS3()
    first = producer(s3, 10)
    original = s3.get_object
    def denied(Bucket, Key):
        if Key == 'calibration/index.json':
            raise s3.error('AccessDenied')
        return original(Bucket=Bucket, Key=Key)
    s3.get_object = denied
    try:
        first.lambda_handler()
    except RuntimeError as exc:
        assert str(exc) == 'AccessDenied'
    else:
        raise AssertionError('read denial must fail, not overwrite an empty index')
    assert len([k for k in s3.objects if k.startswith('calibration/versions/')]) == 1
    assert 'calibration/index.json' not in s3.objects
    s3.get_object = original
    producer(s3, 12).lambda_handler()
    idx = json.loads(s3.objects['calibration/index.json'])
    assert len(idx['versions']) == 2, 'next real handler repairs orphaned immutable version'


def test_compare_and_swap_exhaustion_retains_immutable_history():
    s3 = suite.MemoryS3()
    original = s3.put_object
    def conflict(**kwargs):
        if kwargs['Key'] == 'calibration/index.json':
            raise s3.error('PreconditionFailed')
        return original(**kwargs)
    s3.put_object = conflict
    try:
        producer(s3, 12).lambda_handler()
    except RuntimeError as exc:
        assert 'exhausted retries' in str(exc)
    else:
        raise AssertionError('exhaustion must be explicit')
    assert len([k for k in s3.objects if k.startswith('calibration/versions/')]) == 1
    s3.put_object = original
    producer(s3, 18).lambda_handler()
    assert len(json.loads(s3.objects['calibration/index.json'])['versions']) == 2


if __name__ == '__main__':
    mod = suite._load()
    suite.test_real_snapshotter_preserves_same_week_and_same_second_versions(mod)
    suite.test_validate_only_snapshotter_writes_nothing(mod)
    for name, test in sorted(globals().copy().items()):
        if name.startswith('test_'):
            test()
            print('PASS', name)
    print('snapshotter: immutable, collision, actual race, orphan recovery, CAS exhaustion and validate-only checks passed')
