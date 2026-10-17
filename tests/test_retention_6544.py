"""Invented lifecycle configurations only. No SDK client or AWS access."""
import ast
import contextlib
import copy
from datetime import datetime, timezone
import importlib.util
import io
import json
import logging
from pathlib import Path
import subprocess
import sys
import types
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
PATH = ROOT / 'aws/ops/STAGED/ops_6544_disable_6541_history_expiry.py'
spec = importlib.util.spec_from_file_location('retention6544', PATH)
op = importlib.util.module_from_spec(spec)
spec.loader.exec_module(op)
CANARY = 'PRIVATE_RESPONSE_OR_EXCEPTION_DO_NOT_PUBLISH'


def fixture(status='Enabled'):
    return {'Rules': [
        {'ID': 'unrelated-transition', 'Status': 'Enabled', 'Filter': {'And': {
            'Prefix': 'data/', 'Tags': [{'Key': 'invented', 'Value': CANARY}], 'ObjectSizeGreaterThan': 7}},
         'Transitions': [{'Days': 37, 'StorageClass': 'GLACIER'}],
         'AbortIncompleteMultipartUpload': {'DaysAfterInitiation': 4}},
        {**copy.deepcopy(op.RULE_BODY), 'Status': status},
        {'ID': 'old-history-rule', 'Status': 'Enabled', 'Prefix': 'system-events/',
         'NoncurrentVersionExpiration': {'NoncurrentDays': 91},
         'Expiration': {'ExpiredObjectDeleteMarker': True}},
        {'ID': 'fixed-date', 'Status': 'Disabled', 'Filter': {'Prefix': 'invented/'},
         'Expiration': {'Date': datetime(2035, 1, 1, tzinfo=timezone.utc)}},
    ], 'TransitionDefaultMinimumObjectSize': 'varies_by_storage_class',
        'ResponseMetadata': {'RequestId': CANARY}}


class FakeS3:
    def __init__(self, initial=None):
        self.value = copy.deepcopy(initial if initial is not None else fixture())
        self.calls, self.replies = [], []
        self.put_error = None
        self.apply_put = True

    def get_bucket_lifecycle_configuration(self, **args):
        self.calls.append(('get', args))
        if self.replies:
            reply = self.replies.pop(0)
            if isinstance(reply, BaseException):
                raise reply
            return copy.deepcopy(reply)
        return copy.deepcopy(self.value)

    def put_bucket_lifecycle_configuration(self, **args):
        self.calls.append(('put', copy.deepcopy(args)))
        if self.apply_put:
            self.value = {**copy.deepcopy(args['LifecycleConfiguration']),
                          'TransitionDefaultMinimumObjectSize': args['TransitionDefaultMinimumObjectSize']}
        if self.put_error:
            raise self.put_error
        return {'ResponseMetadata': {'RequestId': CANARY}}


class RetentionContainmentTests(unittest.TestCase):
    def run_case(self, fake, **kwargs):
        result = op.contain(fake, sleep=lambda _: None, **kwargs)
        self.assertNotIn(CANARY, json.dumps(result, default=str))
        self.assertLessEqual(sum(kind == 'put' for kind, args in fake.calls), 1)
        for kind, args in fake.calls:
            self.assertEqual(args['Bucket'], op.BUCKET)
            self.assertEqual(args['ExpectedBucketOwner'], op.OWNER)
        return result

    def test_exact_one_field_change_preserves_all_other_rules_order_dates_and_header(self):
        initial = fixture(); fake = FakeS3(initial)
        result = self.run_case(fake)
        self.assertTrue(result['verified'])
        self.assertEqual(result['reads'], 4)
        self.assertEqual(result['put_attempts'], 1)
        expected = copy.deepcopy(initial)
        expected.pop('ResponseMetadata')
        expected['Rules'][1]['Status'] = 'Disabled'
        self.assertEqual(fake.value, expected)
        self.assertEqual(initial, fixture())
        self.assertEqual(result['target_before']['Status'], 'Enabled')
        self.assertEqual(result['target_after']['Status'], 'Disabled')
        self.assertTrue(result['other_rules_and_fields_unchanged'])

    def test_repeated_disabled_and_read_only_verification_never_put(self):
        for verify in [False, True]:
            fake = FakeS3(fixture('Disabled'))
            result = self.run_case(fake, verify_only=verify)
            self.assertTrue(result['verified'])
            self.assertEqual(result['put_attempts'], 0)
            self.assertTrue(all(kind == 'get' for kind, args in fake.calls))

    def test_verify_enabled_refuses_without_put(self):
        result = self.run_case(FakeS3(), verify_only=True)
        self.assertFalse(result['verified'])
        self.assertEqual(result['failure_code'], 'VERIFY_TARGET_STILL_ENABLED')
        self.assertEqual(result['put_attempts'], 0)

    def test_missing_duplicate_or_changed_target_is_never_guessed(self):
        cases = []
        value = fixture(); value['Rules'].pop(1); cases.append(value)
        value = fixture(); value['Rules'].append(copy.deepcopy(value['Rules'][1])); cases.append(value)
        for change in [lambda r: r.pop('Filter'), lambda r: r.update(Filter={'And': {'Prefix': ''}}),
                       lambda r: r.update(Filter={'Prefix': 'data/'}),
                       lambda r: r['NoncurrentVersionExpiration'].update(NoncurrentDays=2),
                       lambda r: r['NoncurrentVersionExpiration'].pop('NewerNoncurrentVersions'),
                       lambda r: r.update(Expiration={'Days': 5}),
                       lambda r: r.update(Transitions=[{'Days': 9, 'StorageClass': 'GLACIER'}]),
                       lambda r: r.update(Status='unknown')]:
            value = fixture(); change(value['Rules'][1]); cases.append(value)
        for value in cases:
            with self.subTest(value=value['Rules'][0]['ID']):
                fake = FakeS3(value); result = self.run_case(fake)
                self.assertFalse(result['verified'])
                self.assertEqual(result['put_attempts'], 0)

    def test_missing_transition_default_is_not_invented_for_write(self):
        value = fixture(); value.pop('TransitionDefaultMinimumObjectSize')
        result = self.run_case(FakeS3(value))
        self.assertEqual(result['failure_code'], 'TRANSITION_DEFAULT_UNVERIFIED')
        self.assertEqual(result['put_attempts'], 0)

    def test_unknown_top_level_or_unrepresentable_fields_abort_before_put(self):
        for change in [lambda d: d.update(FutureSetting=CANARY),
                       lambda d: d['Rules'][0].update(UnknownSDKField=CANARY)]:
            value = fixture(); change(value); fake = FakeS3(value)
            def validate(args):
                if 'UnknownSDKField' in args['LifecycleConfiguration']['Rules'][0]:
                    raise ValueError(CANARY)
            result = self.run_case(fake, validate=validate)
            self.assertFalse(result['verified'])
            self.assertEqual(result['put_attempts'], 0)
            self.assertEqual(fake.value, value)

    def test_fresh_read_drift_preserves_concurrent_owner_change_without_write(self):
        changed = fixture(); changed['Rules'][0]['Transitions'][0]['Days'] = 99
        fake = FakeS3(); fake.replies = [fixture(), changed]
        result = self.run_case(fake)
        self.assertEqual(result['failure_code'], 'CONCURRENT_CONFIGURATION_CHANGE')
        self.assertEqual(result['put_attempts'], 0)

    def test_request_metadata_changes_are_not_configuration_drift(self):
        a, b = fixture(), fixture(); b['ResponseMetadata'] = {'RequestId': 'DIFFERENT'}
        fake = FakeS3(); fake.replies = [a, b]
        self.assertTrue(self.run_case(fake)['verified'])

    def test_read_errors_and_validation_failure_publish_no_sdk_text(self):
        for error in [PermissionError(CANARY), TimeoutError(CANARY), SystemExit(CANARY), KeyboardInterrupt()]:
            fake = FakeS3(); fake.replies = [error]
            result = self.run_case(fake)
            self.assertFalse(result['verified'])
            self.assertEqual(result['put_attempts'], 0)

    def test_ambiguous_applied_put_is_verified_without_retry(self):
        fake = FakeS3(); fake.put_error = TimeoutError(CANARY)
        result = self.run_case(fake)
        self.assertTrue(result['verified'])
        self.assertEqual(result['put_outcome'], 'AMBIGUOUS_RESPONSE')
        self.assertEqual(result['put_attempts'], 1)

    def test_failed_unapplied_put_stays_incomplete_no_retry_or_rollback(self):
        fake = FakeS3(); fake.apply_put = False; fake.put_error = PermissionError(CANARY)
        result = self.run_case(fake)
        self.assertFalse(result['verified'])
        self.assertEqual(result['failure_code'], 'READBACK_NOT_CONFIRMED')
        self.assertEqual(result['put_attempts'], 1)

    def test_readback_requires_two_matches_and_rejects_other_rule_drift(self):
        before = fixture(); desired = fixture('Disabled'); changed = fixture('Disabled')
        changed['Rules'][0]['Transitions'][0]['Days'] = 88
        fake = FakeS3(); fake.replies = [before, before, desired, before, desired, desired]
        result = self.run_case(fake)
        self.assertTrue(result['verified'])
        self.assertEqual(result['reads'], 6)
        fake = FakeS3(); fake.replies = [before, before, desired, changed]
        result = self.run_case(fake)
        self.assertFalse(result['verified'])
        self.assertEqual(result['failure_code'], 'UNEXPECTED_POSTWRITE_CONFIGURATION')
        self.assertEqual(result['put_attempts'], 1)

    def test_retention_scope_only_exposes_requested_safe_configuration(self):
        value = fixture('Disabled')
        value['Rules'].append({'ID': CANARY + '@example.invalid', 'Status': 'Enabled',
                               'Filter': {'And': {'Prefix': 'private@example.invalid',
                                                  'Tags': [{'Key': CANARY, 'Value': CANARY}]}},
                               'NoncurrentVersionExpiration': {'NoncurrentDays': 8}})
        result = self.run_case(FakeS3(value), verify_only=True)
        rows = result['remaining_enabled_expirations']
        self.assertEqual(rows[-1]['id'], 'WITHHELD')
        self.assertEqual(rows[-1]['prefix'], 'WITHHELD')
        self.assertTrue(rows[-1]['tag_or_size_qualified'])
        self.assertEqual(rows[0]['prefix'], 'system-events/')

    def test_read_only_entrypoint_cannot_select_mutating_mode(self):
        text = (PATH.parent / 'verify_6544_history_expiry.py').read_text()
        self.assertIn('main(verify_only=True)', text)
        self.assertNotIn('boto3', text)

    def test_nonrunner_cli_is_nonzero_without_sdk_or_environment_echo(self):
        p = subprocess.run([sys.executable, '-S', str(PATH)], capture_output=True, text=True,
                           env={'AWS_ACCESS_KEY_ID': CANARY}, check=False)
        self.assertEqual(p.returncode, 1)
        self.assertEqual(p.stderr, '')
        self.assertNotIn(CANARY, p.stdout)
        self.assertEqual(json.loads(p.stdout)['failure_code'], 'RUNNER_REQUIRED')

    def test_runner_boundary_suppresses_transport_stdout_stderr_logging_and_exceptions(self):
        for fail in [False, True]:
            fake = FakeS3()
            if fail:
                fake.replies = [TimeoutError(CANARY)]
            meta = types.SimpleNamespace(operation_model=lambda _: types.SimpleNamespace(input_shape=object()))
            fake.meta = types.SimpleNamespace(service_model=meta)
            stream = io.StringIO(); handler = logging.StreamHandler(stream)
            logger = logging.getLogger('synthetic-retention-sdk'); logger.addHandler(handler)
            sdk = types.ModuleType('boto3')
            def make_client(*args, **kwargs):
                print(CANARY); print(CANARY, file=sys.stderr); logger.critical(CANARY)
                return fake
            sdk.client = make_client
            package = types.ModuleType('botocore'); package.__path__ = []
            config = types.ModuleType('botocore.config'); config.Config = lambda **kwargs: kwargs
            validation = types.ModuleType('botocore.validate'); validation.validate_parameters = lambda *args: None
            original = op.contain
            previous = logging.root.manager.disable
            out, err, files = io.StringIO(), io.StringIO(), []
            try:
                with patch.dict(sys.modules, {'boto3': sdk, 'botocore': package,
                     'botocore.config': config, 'botocore.validate': validation}), \
                     patch.dict(op.os.environ, {'GITHUB_ACTIONS': 'true', 'GITHUB_REPOSITORY': 'ElMooro/si',
                                                'GITHUB_SHA': 'a' * 40}, clear=True), \
                     patch.object(op.signal, 'signal'), patch.object(op.signal, 'alarm'), \
                     patch.object(op, 'contain', side_effect=lambda *a, **kw: original(*a, **kw, sleep=lambda _: None)), \
                     patch.object(Path, 'write_text', side_effect=lambda value, **kw: files.append(value)), \
                     contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
                    code = op.main()
                self.assertEqual(code, 1 if fail else 0)
                self.assertEqual(err.getvalue() + stream.getvalue(), '')
                self.assertNotIn(CANARY, out.getvalue() + ''.join(files))
                self.assertEqual(logging.root.manager.disable, previous)
                self.assertEqual(json.loads(out.getvalue())['verified'], not fail)
            finally:
                logger.removeHandler(handler)

    def test_retired_6541_section_is_read_only_for_every_status(self):
        source = (PATH.parent / 'ops_6541_arm_runaway_guardrails.py').read_text()
        tree = ast.parse(source)
        node = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == 'lifecycle')
        code = compile(ast.Module(body=[node], type_ignores=[]), '<isolated-6541-section>', 'exec')
        for status in ['Enabled', 'Disabled', None]:
            fake = FakeS3(fixture(status or 'Disabled'))
            if status is None:
                fake.value['Rules'].pop(1)
            scope = {'s3': fake, 'LIVE_BUCKET': op.BUCKET}; exec(code, scope)
            reporter = types.SimpleNamespace(ok=lambda _: None)
            if status == 'Enabled':
                with self.assertRaises(RuntimeError): scope['lifecycle'](reporter)
            else:
                result = scope['lifecycle'](reporter)
                self.assertFalse(result['lifecycle_write'])
            self.assertTrue(all(kind == 'get' for kind, args in fake.calls))


if __name__ == '__main__':
    unittest.main()
