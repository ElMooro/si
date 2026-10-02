"""Invented SDK/package fixtures only; no AWS, retained archives or transport."""
from contextlib import contextmanager
import copy
import hashlib
import importlib.util
import json
from pathlib import Path
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[2]


def load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


capture = load('postrelease_capture_fixture', ROOT / 'tests/ops/test_series_reliability_receipt_bound_capture.py')
p = load('pr81_postrelease', ROOT / 'aws/ops/staged/ops_6474_series_pr81_postrelease_acceptance.py')


class Fixture(capture.Fixture):
    def __enter__(self):
        super().__enter__()
        retained = super().inspect()
        self.stack.enter_context(patch.object(p, 'ROOT', self.root))
        self.stack.enter_context(patch.object(p.h, 'ROOT', self.root))
        self.stack.enter_context(patch.object(p, 'BASELINE', self.baseline))
        self.stack.enter_context(patch.object(p, 'EXPECTED_RELEASE_COMMIT', capture.old.RECEIPT_COMMIT))
        self.stack.enter_context(patch.object(p, 'EXPECTED_RELEASE_RUN', capture.p.INSTALLED_RELEASE_RUN))
        checksum = hashlib.sha256(capture.old.CANDIDATE).hexdigest()
        sources = dict(p.h.RELEASE_SOURCES, PR81=checksum)
        self.stack.enter_context(patch.object(p, 'SOURCE_SHA256', checksum))
        self.stack.enter_context(patch.object(p.h, 'RELEASE_SOURCES', sources))
        retained['approved_release_sources'] = sources
        self.baseline.write_bytes(json.dumps(retained, sort_keys=True).encode() + b'\n')
        self.stack.enter_context(patch.object(p, 'BASELINE_SHA256', hashlib.sha256(self.baseline.read_bytes()).hexdigest()))
        self.stack.enter_context(patch.object(p.h, 'package_source_evidence', return_value={'invented': True}))
        self.source.joinpath('lambda_function.py').write_bytes(capture.old.CANDIDATE)
        self.set_package(capture.old.CANDIDATE)
        self.git_source = capture.old.CANDIDATE
        self.make_receipt()
        self.calls.clear()
        self.signed_gets.clear()
        return self

    def inspect(self):
        return p.inspect(self, self, self, self, p.Reader(capture.old.SDKError, (TimeoutError,)),
                         opener=self.opener)


class Report:
    def __init__(self):
        self.logs, self.rows = [], []

    def log(self, text):
        self.logs.append(text)

    def kv(self, **row):
        self.rows.append(row)


@contextmanager
def report_for(out):
    yield out


class Acceptance(unittest.TestCase):
    def stop(self, fixture, reason):
        with self.assertRaisesRegex(p.Stop, '^' + reason + '$'):
            fixture.inspect()

    def test_healthy_scope_exact_bytes_controls_and_separate_qualifications(self):
        with Fixture() as f:
            result = f.inspect()
            self.assertTrue(result['technical_acceptance_qualified'])
            self.assertFalse(result['release_qualified'])
            self.assertFalse(result['natural_output_qualified'])
            self.assertFalse(result['PR78_permission_qualified'])
            self.assertEqual(result['aws_read_calls'], 17)
            self.assertEqual(result['operating_controls'], json.loads(f.baseline.read_bytes())['operating_controls'])
            self.assertEqual(result['intended_release_run'], f.receipt['run_id'])
            self.assertEqual(f.signed_gets, [('PRIVATE_SIGNED_URL', 30)])
            self.assertEqual([name for name, _ in f.calls].count('get_function'), 2)
            self.assertEqual([name for name, _ in f.calls].count('get_runtime_management_config'), 2)
            self.assertEqual([kwargs for name, kwargs in f.calls if name == 'get_object'],
                             [{'Bucket': p.h.BUCKET, 'Key': p.h.RECEIPT_KEY}])
            self.assertFalse({'simulate_principal_policy', 'list_objects_v2', 'assume_role', 'put_object'}
                             & {name for name, _ in f.calls})
            self.assertNotIn('PRIVATE', json.dumps(result))

    def test_unset_or_malformed_bindings_refuse_before_all_reads(self):
        for name, values in [('EXPECTED_RELEASE_COMMIT', [None, True, '', 'x' * 40, 'A' * 40]),
                             ('EXPECTED_RELEASE_RUN', [None, True, 123, '', ' 123', '0', '1' * 21]),
                             ('PHASE', ['PR78', 'combined', None]), ('SOURCE_SHA256', ['changed'])]:
            for value in values:
                with self.subTest(name=name, value=value), Fixture() as f, patch.object(p, name, value):
                    with self.assertRaises(p.Stop):
                        f.inspect()
                    self.assertEqual(f.calls, [])
                    self.assertEqual(f.signed_gets, [])

    def test_baseline_and_checkout_admission_before_reads(self):
        with Fixture() as f:
            f.baseline.write_bytes(f.baseline.read_bytes() + b' ')
            self.stop(f, 'prospective_baseline_bytes_changed')
            self.assertEqual(f.calls, [])
        with Fixture() as f:
            f.source.joinpath('lambda_function.py').write_bytes(b'invented altered handler')
            self.stop(f, 'checkout_handler_not_exact_PR81')
            self.assertEqual(f.calls, [])

    def test_receipt_commit_run_typed_hash_size_and_verified_are_strict(self):
        mutations = [('commit', 'b' * 40), ('run_id', None), ('run_id', 123), ('run_id', True),
                     ('run_id', 'different'), ('verified', 1), ('schema', 'different'),
                     ('function', 'different'), ('zip_bytes', True), ('zip_bytes', 1),
                     ('zip_sha256_hex', 'bad'), ('code_sha256', 'bad')]
        for field, value in mutations:
            with self.subTest(field=field, value=value), Fixture() as f:
                f.receipt[field] = value
                self.stop(f, 'intended_release_receipt_not_exact')
        with Fixture() as f:
            f.receipt = None
            self.stop(f, 'release_receipt_missing')

    def test_package_source_and_stream_failures_cannot_qualify(self):
        with Fixture() as f:
            f.raw += b'invented changed bytes'
            self.stop(f, 'package_hash_mismatch')
        with Fixture() as f:
            f.set_package(capture.old.CANDIDATE, extra=capture.old.CANDIDATE)
            self.stop(f, 'source_member_not_unique')
        with Fixture() as f:
            f.receipt['source']['lambda_function.py']['bytes'] = True
            self.stop(f, 'receipt_handler_mismatch')
        with Fixture() as f, patch.object(p.h, 'package_source_evidence',
                side_effect=p.Stop('package_repository_source_mismatch')):
            self.stop(f, 'package_repository_source_mismatch')
        with Fixture() as f:
            def lost_stream(*_args, **_kwargs):
                raise TimeoutError('PRIVATE_SIGNED_LOCATION')
            with patch.object(f, 'opener', lost_stream):
                with self.assertRaises(TimeoutError):
                    f.inspect()

    def test_entire_prospective_runtime_controls_binding_and_private_projection_are_exact(self):
        changes = [
            lambda f: f.live['RuntimeVersionConfig'].update(RuntimeVersionArn=capture.ARN[:-1] + 'b'),
            lambda f: f.management.update(UpdateRuntimeOn='FunctionUpdate'),
            lambda f: f.live['Environment']['Variables'].update(PRIVATE_ADDITIONAL='PRIVATE_CHANGED'),
            lambda f: f.targets[p.h.CLASSIC[0]]['Targets'][0]['RetryPolicy'].update(MaximumRetryAttempts=3),
            lambda f: f.rules[p.h.CLASSIC[1]].update(ScheduleExpression='rate(3 minutes)'),
            lambda f: f.schedule['Target'].update(Input='PRIVATE_CHANGED')]
        for change in changes:
            with self.subTest(change=changes.index(change)), Fixture() as f:
                change(f)
                self.stop(f, 'prospective_controls_changed_acceptance_held')
        for value in [True, False, 0, 2, 1.0, None]:
            with self.subTest(concurrency=value), Fixture() as f:
                f.concurrency = {'ReservedConcurrentExecutions': value}
                self.stop(f, 'reserved_concurrency_not_one')

    def test_failed_reads_and_second_observation_cannot_qualify(self):
        for method in ['get_function', 'get_runtime_management_config', 'get_function_concurrency',
                       'describe_rule', 'list_targets_by_rule', 'get_schedule', 'get_object']:
            for error in [capture.old.SDKError('AccessDenied'), TimeoutError('PRIVATE_ERROR'),
                          ValueError('PRIVATE_ERROR')]:
                with self.subTest(method=method, error=type(error).__name__), Fixture() as f:
                    f.errors[method] = error
                    with self.assertRaises(p.Stop) as stopped:
                        f.inspect()
                    self.assertNotIn('PRIVATE', str(stopped.exception))
        with Fixture() as f:
            original = f.get_function
            count = [0]
            def changed(**kwargs):
                item = original(**kwargs)
                count[0] += 1
                if count[0] == 2:
                    item['Configuration']['CodeSha256'] = 'changed'
                return item
            changed.__name__ = 'get_function'
            with patch.object(f, 'get_function', changed):
                self.stop(f, 'observations_changed')

    def test_deadline_inside_sdk_is_not_swallowed_and_scope_bound_is_strict(self):
        with Fixture() as f:
            f.errors['get_function'] = p.TimeBound()
            with self.assertRaises(p.TimeBound):
                f.inspect()
        with Fixture() as f:
            reader = p.Reader()
            with self.assertRaisesRegex(p.Stop, 'postrelease_read_scope_not_allowed'):
                reader.read(f.simulate_principal_policy, **p.h.simulation_requests()[0])
            self.assertEqual(reader.calls, 0)
            reader.calls = 17
            with self.assertRaisesRegex(p.Stop, 'api_bound_reached'):
                reader.read(f.get_function, FunctionName=p.h.FUNCTION)
            self.assertEqual(f.calls, [])

    def test_full_main_healthy_and_runtime_error_outputs_remain_private(self):
        import boto3
        import sys
        sys.path.insert(0, str(ROOT / 'aws/ops'))
        import ops_report
        for runtime_error in [False, True]:
            with self.subTest(runtime_error=runtime_error), Fixture() as f:
                if runtime_error:
                    f.live['RuntimeVersionConfig']['Error'] = {'Message': 'PRIVATE_SECRET'}
                out = Report()
                inspect = p.inspect
                with patch.dict('os.environ', {'GITHUB_ACTIONS': 'true'}), \
                     patch.object(boto3, 'client', return_value=f), \
                     patch.object(ops_report, 'report', side_effect=lambda _: report_for(out)), \
                     patch.object(p.signal, 'alarm'), patch.object(p.signal, 'signal'), \
                     patch.object(p, 'inspect', side_effect=lambda *args: inspect(*args, opener=f.opener)):
                    if runtime_error:
                        with self.assertRaises(SystemExit):
                            p.main()
                    else:
                        p.main()
                self.assertNotIn('PRIVATE', json.dumps(out.logs + out.rows))
                self.assertEqual(out.rows[-1]['completed'], not runtime_error)
                if not runtime_error:
                    self.assertEqual(out.rows[-1]['aws_read_calls'], 17)
                    result = json.loads(out.logs[0].split('POSTRELEASE_JSON ', 1)[1])
                    self.assertFalse(result['release_qualified'])
                else:
                    self.assertIn('RUNTIME_UNQUALIFIED_JSON ', out.logs[0])

    def test_full_main_unbound_no_clients_and_untrusted_stop_details_withheld(self):
        import boto3
        import sys
        sys.path.insert(0, str(ROOT / 'aws/ops'))
        import ops_report
        for failure in [p.Stop('PRIVATE_SECRET'), ValueError('PRIVATE_SECRET'), p.TimeBound()]:
            with self.subTest(failure=type(failure).__name__), Fixture() as f:
                out = Report()
                with patch.dict('os.environ', {'GITHUB_ACTIONS': 'true'}), \
                     patch.object(boto3, 'client', return_value=f), \
                     patch.object(ops_report, 'report', side_effect=lambda _: report_for(out)), \
                     patch.object(p.signal, 'alarm'), patch.object(p.signal, 'signal'), \
                     patch.object(p, 'inspect', side_effect=failure), self.assertRaises(SystemExit):
                    p.main()
                self.assertNotIn('PRIVATE', json.dumps(out.logs + out.rows))
                self.assertFalse(out.rows[-1]['completed'])
        out = Report()
        with patch.dict('os.environ', {'GITHUB_ACTIONS': 'true'}), \
             patch.object(boto3, 'client') as client, \
             patch.object(ops_report, 'report', side_effect=lambda _: report_for(out)), \
             patch.object(p.signal, 'alarm'), patch.object(p.signal, 'signal'), self.assertRaises(SystemExit):
            p.main()
        client.assert_not_called()
        self.assertEqual(out.rows[-1]['aws_read_calls'], 0)


if __name__ == '__main__':
    unittest.main(verbosity=2)
