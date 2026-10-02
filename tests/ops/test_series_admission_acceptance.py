"""Dependency-free invented SDK/package tests; no live AWS or provider calls."""
import base64
from contextlib import ExitStack, contextmanager
import hashlib
import importlib.util
import io
import json
from pathlib import Path
import tempfile
import types
import unittest
from unittest.mock import patch
import zipfile

ROOT = Path(__file__).resolve().parents[2]
PATH = ROOT / 'aws/ops/staged/ops_6450_series_admission_acceptance.py'
spec = importlib.util.spec_from_file_location('series_admission_acceptance', PATH)
probe = importlib.util.module_from_spec(spec)
spec.loader.exec_module(probe)
PREDECESSOR = b'invented predecessor handler\n'
CANDIDATE = b'invented reviewed candidate handler\n'
RECEIPT_COMMIT = 'f' * 40


class SDKError(Exception):
    def __init__(self, code):
        self.response = {'Error': {'Code': code}}
        super().__init__('PRIVATE_ERROR_DETAILS')


class Fixture:
    def __init__(self):
        self.temp = tempfile.TemporaryDirectory(prefix='series-admission-probe-')
        self.root = Path(self.temp.name)
        self.source = self.root / 'aws/lambdas' / probe.FUNCTION / 'source'
        self.source.mkdir(parents=True)
        (self.source / 'lambda_function.py').write_bytes(CANDIDATE)
        self.cfg = json.loads((ROOT / 'aws/lambdas' / probe.FUNCTION / 'config.json').read_bytes())
        (self.source.parent / 'config.json').write_text(json.dumps(self.cfg), encoding='utf-8')
        self.baseline = self.root / 'baseline.json'
        self.calls = []
        self.signed_gets = []
        self.live = {'FunctionName': probe.FUNCTION, 'FunctionArn': 'arn:aws:lambda:us-east-1:857687956942:function:' + probe.FUNCTION,
                     'State': 'Active', 'LastUpdateStatus': 'Successful',
                     'Runtime': self.cfg['runtime'], 'Handler': self.cfg['handler'],
                     'MemorySize': self.cfg['memory'], 'Timeout': self.cfg['timeout'],
                     'Description': self.cfg['description'], 'Role': self.cfg['role'],
                     'Environment': {'Variables': {**self.cfg['env'], 'PRIVATE_ENV_NAME': 'PRIVATE_ENV_VALUE'}},
                     'TracingConfig': {'Mode': 'Active'}, 'DeadLetterConfig': {'TargetArn': probe.DEFAULT_DLQ},
                     'EphemeralStorage': {'Size': 512}, 'Architectures': ['x86_64'], 'PackageType': 'Zip',
                     'VpcConfig': {'SubnetIds': ['PRIVATE_SUBNET']}, 'LoggingConfig': {'LogGroup': 'PRIVATE_LOG_GROUP'}}
        self.rules = {name: {'Name': name, 'State': 'ENABLED', 'ScheduleExpression': 'rate(2 minutes)',
                            'Description': 'PRIVATE_RULE_DESCRIPTION', 'ResponseMetadata': {'RequestId': 'PRIVATE_REQUEST'}}
                      for name in probe.CLASSIC}
        self.targets = {name: {'Targets': [{'Id': 'target-1', 'Arn': self.live['FunctionArn'],
                          'Input': '{"provider":"eurostat","PRIVATE_PAYLOAD":"PRIVATE_TARGET_VALUE"}',
                          'RoleArn': 'PRIVATE_ROLE_ARN', 'RetryPolicy': {'MaximumRetryAttempts': 2},
                          'DeadLetterConfig': {'Arn': 'PRIVATE_DLQ'}}]} for name in probe.CLASSIC}
        self.targets[probe.CLASSIC[0]]['Targets'].append({'Id': 'target-2', 'Arn': self.live['FunctionArn'],
                                                       'Input': '{"provider":"ecb"}'})
        self.schedule = {'Name': probe.SCHEDULER, 'State': 'ENABLED', 'ScheduleExpression': 'rate(5 minutes)',
                         'ScheduleExpressionTimezone': 'UTC', 'FlexibleTimeWindow': {'Mode': 'OFF'},
                         'Target': {'Arn': 'PRIVATE_SCHEDULER_ARN', 'Input': 'PRIVATE_SCHEDULER_PAYLOAD',
                                    'RoleArn': 'PRIVATE_SCHEDULER_ROLE'},
                         'CreationDate': '2026-01-01', 'LastModificationDate': '2026-01-01',
                         'ResponseMetadata': {'RequestId': 'PRIVATE_REQUEST'}}
        self.errors = {}
        self.receipt = None
        self.git_source = CANDIDATE
        self.set_package(PREDECESSOR)
        self.clients = (self, self, self, self)

    def __enter__(self):
        self.stack = ExitStack()
        self.stack.enter_context(self.temp)
        self.stack.enter_context(patch.object(probe, 'ROOT', self.root))
        self.stack.enter_context(patch.object(probe, 'BASELINE', self.baseline))
        self.stack.enter_context(patch.object(probe, 'PREDECESSOR_SHA256', hashlib.sha256(PREDECESSOR).hexdigest()))
        self.stack.enter_context(patch.object(probe.subprocess, 'check_output', self.git_show))
        return self

    def __exit__(self, *args):
        return self.stack.__exit__(*args)

    def set_package(self, actual, extra=None):
        stream = io.BytesIO()
        with zipfile.ZipFile(stream, 'w', compression=zipfile.ZIP_DEFLATED) as archive:
            archive.writestr('lambda_function.py', actual)
            if extra is not None:
                archive.writestr('lambda_function.py', extra)
        self.raw = stream.getvalue()
        self.live.update(CodeSha256=base64.b64encode(hashlib.sha256(self.raw).digest()).decode(), CodeSize=len(self.raw))
        self.actual = actual

    def record(self, name, kwargs):
        self.calls.append((name, kwargs))
        key = (name, kwargs.get('Name') or kwargs.get('Rule'))
        error = self.errors.get(key, self.errors.get(name))
        if error:
            raise error

    def get_function(self, **kwargs):
        self.record('get_function', kwargs)
        return {'Configuration': self.live, 'Code': {'Location': 'PRIVATE_SIGNED_URL'}}

    def get_function_concurrency(self, **kwargs):
        self.record('get_function_concurrency', kwargs)
        return getattr(self, 'concurrency', {'ReservedConcurrentExecutions': 1})

    def get_object(self, **kwargs):
        self.record('get_object', kwargs)
        if self.receipt is None:
            raise SDKError('NoSuchKey')
        return {'Body': io.BytesIO(json.dumps(self.receipt).encode())}

    def describe_rule(self, **kwargs):
        self.record('describe_rule', kwargs)
        return self.rules[kwargs['Name']]

    def list_targets_by_rule(self, **kwargs):
        self.record('list_targets_by_rule', kwargs)
        return self.targets[kwargs['Rule']]

    def get_schedule(self, **kwargs):
        self.record('get_schedule', kwargs)
        return self.schedule

    def git_show(self, command, **kwargs):
        assert command[:2] == ['git', 'show']
        assert kwargs['timeout'] == 15
        if command[2].startswith(probe.PREDECESSOR_COMMIT + ':'):
            return PREDECESSOR
        assert command[2].startswith(RECEIPT_COMMIT + ':')
        return self.git_source

    def opener(self, url, timeout):
        self.signed_gets.append((url, timeout))
        return io.BytesIO(self.raw)

    def inspect(self):
        return probe.inspect(*self.clients, probe.Reader(SDKError), opener=self.opener)

    def make_receipt(self):
        self.receipt = {'schema': 'release-receipt.v1', 'function': probe.FUNCTION, 'verified': True,
                        'commit': RECEIPT_COMMIT, 'code_sha256': self.live['CodeSha256'],
                        'zip_bytes': len(self.raw), 'zip_sha256_hex': hashlib.sha256(self.raw).hexdigest(),
                        'deployed_at': '2026-10-02T01:02:03Z', 'source': {'lambda_function.py': {
                            'sha256': hashlib.sha256(self.actual).hexdigest(), 'bytes': len(self.actual)}}}

    def candidate(self):
        baseline = self.inspect()
        self.baseline.write_text(json.dumps(baseline), encoding='utf-8')
        self.calls.clear()
        self.signed_gets.clear()
        self.set_package(CANDIDATE)
        self.make_receipt()


class ProbeTests(unittest.TestCase):
    def stop(self, fixture, reason):
        with self.assertRaisesRegex(probe.Stop, '^' + reason + '$'):
            fixture.inspect()

    def test_predecessor_and_candidate_exact_bounded_traces_are_private(self):
        for candidate in (False, True):
            with self.subTest(candidate=candidate), Fixture() as f:
                if candidate:
                    f.candidate()
                result = f.inspect()
                self.assertEqual(result['source_phase'], 'candidate' if candidate else 'predecessor')
                self.assertEqual(result['handler_sha256'], hashlib.sha256(f.actual).hexdigest())
                self.assertEqual(result['zip_bytes'], len(f.raw))
                self.assertEqual(result['aws_read_calls'], 14)
                self.assertEqual(result['named_bindings_checked'], 6)
                self.assertEqual(result['aws_writes'], 0)
                self.assertEqual(result['producer_invokes'], 0)
                self.assertEqual(f.signed_gets, [('PRIVATE_SIGNED_URL', 30)])
                expected = [('get_function', {'FunctionName': probe.FUNCTION}),
                            ('get_function_concurrency', {'FunctionName': probe.FUNCTION})]
                for name in probe.CLASSIC:
                    expected.extend([('describe_rule', {'Name': name}),
                                     ('list_targets_by_rule', {'Rule': name, 'Limit': 100})])
                expected.extend([('get_schedule', {'Name': probe.SCHEDULER, 'GroupName': 'default'}),
                                 ('get_object', {'Bucket': probe.BUCKET, 'Key': probe.RECEIPT_KEY})])
                self.assertEqual(f.calls, expected)
                self.assertNotIn('PRIVATE', json.dumps(result))

    def test_candidate_needs_true_unchanged_predecessor_baseline_and_receipt(self):
        with Fixture() as f:
            f.set_package(CANDIDATE)
            self.stop(f, 'candidate_baseline_missing')
        for change, reason in ((lambda f: f.baseline.unlink(), 'candidate_baseline_missing'),
                               (lambda f: f.baseline.write_text('{}', encoding='utf-8'), 'baseline_invalid'),
                               (lambda f: setattr(f, 'receipt', None), 'candidate_receipt_missing')):
            with self.subTest(reason=reason), Fixture() as f:
                f.candidate()
                change(f)
                self.stop(f, reason)

    def test_unreviewed_handler_duplicate_or_oversize_member_stops(self):
        with Fixture() as f:
            f.set_package(b'unreviewed arbitrary code')
            self.stop(f, 'live_handler_not_reviewed')
        with Fixture() as f:
            with self.assertWarns(UserWarning):
                f.set_package(PREDECESSOR, extra=PREDECESSOR)
            self.stop(f, 'source_member_not_unique')
        with Fixture() as f:
            f.set_package(b'x' * (probe.SOURCE_BOUND + 1))
            self.stop(f, 'source_member_byte_bound')

    def test_package_code_hash_size_and_source_pin_stop(self):
        for key, value in (('CodeSha256', 'bad'), ('CodeSize', 0)):
            with self.subTest(key=key), Fixture() as f:
                f.live[key] = value
                self.stop(f, 'package_hash_mismatch')
        with Fixture() as f, patch.object(probe, 'PREDECESSOR_SHA256', '0' * 64):
            self.stop(f, 'predecessor_source_pin_mismatch')

    def test_ready_and_strict_reserved_concurrency_gate(self):
        for field in ('State', 'LastUpdateStatus', 'FunctionName', 'FunctionArn'):
            with self.subTest(field=field), Fixture() as f:
                f.live[field] = 'unexpected'
                self.stop(f, 'function_not_ready')
        for value in ({}, {'ReservedConcurrentExecutions': True}, {'ReservedConcurrentExecutions': 0},
                      {'ReservedConcurrentExecutions': 2}, {'ReservedConcurrentExecutions': 1.0}):
            with self.subTest(value=value), Fixture() as f:
                f.concurrency = value
                self.stop(f, 'reserved_concurrency_not_one')

    def test_every_reapplied_config_control_must_already_match(self):
        cases = [('MemorySize', 128, 'memory_matches'), ('Timeout', 30, 'timeout_matches'),
                 ('Description', 'different', 'description_matches'),
                 ('Runtime', 'python3.11', 'runtime_matches'), ('Handler', 'other.handler', 'handler_matches'),
                 ('Role', 'other-role', 'role_matches'), ('Environment', {'Variables': {}}, 'declared_environment_matches'),
                 ('TracingConfig', {'Mode': 'PassThrough'}, 'tracing_already_active'),
                 ('DeadLetterConfig', {}, 'dlq_already_standard')]
        for field, value, reason in cases:
            with self.subTest(field=field), Fixture() as f:
                f.live[field] = value
                self.stop(f, 'release_control_mismatch:' + reason)
        with Fixture() as f:
            f.live['Environment']['Error'] = {'Message': 'PRIVATE_ERROR'}
            self.stop(f, 'live_environment_unavailable')

    def test_legacy_ephemeral_is_preserved_and_only_canonical_field_is_managed(self):
        with Fixture() as f:
            f.cfg['ephemeral_mb'] = 10240
            (f.source.parent / 'config.json').write_text(json.dumps(f.cfg), encoding='utf-8')
            f.candidate()
            self.assertEqual(f.inspect()['operating_controls']['technical_configuration']['EphemeralStorage'], {'Size': 512})
            f.live['EphemeralStorage']['Size'] = 10240
            self.stop(f, 'operating_controls_changed')
        with Fixture() as f:
            f.cfg['ephemeral_storage'] = 10240
            (f.source.parent / 'config.json').write_text(json.dumps(f.cfg), encoding='utf-8')
            self.stop(f, 'release_control_mismatch:ephemeral_matches_managed_configuration')
        for value in (True, 511, 10241, 512.0, None):
            with self.subTest(value=value), Fixture() as f:
                f.live['EphemeralStorage']['Size'] = value
                self.stop(f, 'live_ephemeral_invalid')

    def test_all_five_intentional_monitors_are_enabled_and_bound(self):
        for name in probe.CLASSIC:
            with self.subTest(name=name), Fixture() as f:
                f.rules[name]['State'] = 'DISABLED'
                self.stop(f, 'named_rule_not_enabled')
            with self.subTest(name=name), Fixture() as f:
                f.targets[name]['Targets'] = []
                self.stop(f, 'target_row_bound_reached')
        with Fixture() as f:
            f.schedule['State'] = 'DISABLED'
            self.stop(f, 'protected_monitor_scheduler_not_enabled')
        with Fixture() as f:
            f.schedule['Target'] = {}
            self.stop(f, 'protected_monitor_scheduler_not_enabled')
        with Fixture() as f:
            for target in f.targets[probe.CLASSIC[0]]['Targets']:
                target['Arn'] = 'different-lambda'
            self.stop(f, 'extractor_rule_unbound')

    def test_pagination_and_too_many_targets_are_rejected(self):
        for alteration in ({'NextToken': 'PRIVATE_TOKEN'}, {'Targets': [{}] * 101}):
            with self.subTest(alteration=alteration), Fixture() as f:
                f.targets[probe.CLASSIC[0]].update(alteration)
                self.stop(f, 'target_row_bound_reached')

    def test_payload_roles_extra_settings_and_full_environment_changes_are_detected(self):
        mutations = [lambda f: f.targets[probe.CLASSIC[0]]['Targets'][0].update(Input='changed'),
                     lambda f: f.targets[probe.CLASSIC[0]]['Targets'][0].update(RoleArn='changed'),
                     lambda f: f.targets[probe.CLASSIC[0]]['Targets'][0].update(RetryPolicy={'MaximumRetryAttempts': 0}),
                     lambda f: f.rules[probe.CLASSIC[1]].update(ScheduleExpression='rate(1 day)'),
                     lambda f: f.live['Environment']['Variables'].update(PRIVATE_ENV_VALUE='changed'),
                     lambda f: f.live.update(Architectures=['arm64']),
                     lambda f: f.live.update(VpcConfig={}),
                     lambda f: f.live.update(LoggingConfig={}),
                     lambda f: f.schedule['Target'].update(Input='changed'),
                     lambda f: f.schedule.update(FlexibleTimeWindow={'Mode': 'FLEXIBLE', 'MaximumWindowInMinutes': 10})]
        for index, mutation in enumerate(mutations):
            with self.subTest(index=index), Fixture() as f:
                f.candidate()
                mutation(f)
                self.stop(f, 'operating_controls_changed')

    def test_drift_diagnostic_preserves_failure_gate_read_bound_and_baseline(self):
        cases = [(lambda f: f.live.update(RuntimeVersionConfig={'RuntimeVersionArn': 'PRIVATE_RUNTIME_ARN'}),
                  'private_configuration.RuntimeVersionConfig'),
                 (lambda f: f.live['Environment']['Variables'].update(PRIVATE_ENV_NAME='PRIVATE_NEW_ENV'),
                  'private_configuration.Environment'),
                 (lambda f: f.targets[probe.CLASSIC[0]]['Targets'][0].update(Input='PRIVATE_NEW_PAYLOAD'),
                  'bindings.' + probe.CLASSIC[0])]
        for mutate, path in cases:
            with self.subTest(path=path), Fixture() as f:
                f.candidate()
                original_baseline = f.baseline.read_bytes()
                mutate(f)
                with self.assertRaises(probe.ControlsChanged) as stopped:
                    f.inspect()
                self.assertEqual(str(stopped.exception), 'operating_controls_changed')
                diagnostic = stopped.exception.diagnostic
                self.assertEqual([row['path'] for row in diagnostic['changed_fields']], [path])
                self.assertEqual(diagnostic['verified_package'], {
                    'source_phase': 'candidate', 'handler_sha256': hashlib.sha256(CANDIDATE).hexdigest(),
                    'code_sha256': f.live['CodeSha256'], 'zip_sha256_hex': hashlib.sha256(f.raw).hexdigest(),
                    'zip_bytes': len(f.raw), 'signed_package_gets': 1, 'function_name_matches': True,
                    'state_active': True, 'last_update_successful': True, 'aws_writes': 0, 'producer_invokes': 0})
                self.assertNotEqual(diagnostic['baseline_fingerprint'], diagnostic['observed_fingerprint'])
                for row in diagnostic['changed_fields']:
                    self.assertRegex(row['before_sha256'], '^[a-f0-9]{64}$')
                    self.assertRegex(row['after_sha256'], '^[a-f0-9]{64}$')
                    self.assertNotEqual(row['before_sha256'], row['after_sha256'])
                self.assertNotIn('PRIVATE', json.dumps(diagnostic))
                if path == 'private_configuration.RuntimeVersionConfig':
                    self.assertEqual(diagnostic['baseline_runtime_version_shape'], 'unavailable_from_retained_digest')
                    self.assertEqual(diagnostic['observed_runtime_version_shape']['value_type'], 'object')
                    self.assertFalse(diagnostic['observed_runtime_version_shape']['runtime_arn_syntax_valid'])
                    self.assertFalse(diagnostic['observed_runtime_version_shape']['error_field_present'])
                else:
                    self.assertNotIn('observed_runtime_version_shape', diagnostic)
                self.assertEqual(f.baseline.read_bytes(), original_baseline)
                self.assertEqual(len(f.calls), 13)
                self.assertNotIn('get_object', [name for name, _ in f.calls])
                self.assertEqual(len(f.signed_gets), 1)
        self.assertEqual(probe.control_differences({'reserved_concurrency': 1}, {'reserved_concurrency': 1}), [])

    def test_runtime_shape_does_not_claim_old_digest_was_an_arn_or_expose_current_values(self):
        arn = 'arn:aws:lambda:us-east-1::runtime:' + 'a' * 64
        cases = [(None, 'null', False, False), ('PRIVATE_SCALAR', 'string', False, False),
                 ([], 'array', False, False), ({}, 'object', False, False),
                 ({'RuntimeVersionArn': arn}, 'object', False, True),
                 ({'Error': {'Message': 'PRIVATE_RUNTIME_ERROR'}}, 'object', True, False),
                 ({'RuntimeVersionArn': 'PRIVATE_BAD_ARN', 'Error': {}}, 'object', True, False),
                 ({'PRIVATE_UNKNOWN_FIELD': 'PRIVATE_UNKNOWN_VALUE'}, 'object', False, False)]
        for value, kind, error, valid in cases:
            with self.subTest(kind=kind, error=error, valid=valid):
                shape = probe.runtime_version_shape(value)
                self.assertEqual(shape['value_type'], kind)
                self.assertEqual(shape['error_field_present'], error)
                self.assertEqual(shape['runtime_arn_syntax_valid'], valid)
                self.assertNotIn('PRIVATE', json.dumps(shape))
                self.assertNotIn(arn, json.dumps(shape))
        with Fixture() as f:
            f.candidate()
            f.live['RuntimeVersionConfig'] = {'RuntimeVersionArn': arn, 'Error': {'Message': 'PRIVATE_RUNTIME_ERROR'}}
            with self.assertRaises(probe.ControlsChanged) as stopped:
                f.inspect()
            diagnostic = stopped.exception.diagnostic
            self.assertEqual(diagnostic['baseline_runtime_version_shape'], 'unavailable_from_retained_digest')
            self.assertTrue(diagnostic['observed_runtime_version_shape']['runtime_arn_syntax_valid'])
            self.assertTrue(diagnostic['observed_runtime_version_shape']['error_field_present'])
            self.assertNotIn(arn, json.dumps(diagnostic))
            self.assertNotIn('PRIVATE', json.dumps(diagnostic))

    def test_diff_paths_do_not_expose_unknown_baseline_field_names(self):
        before = {'private_configuration': {'PRIVATE_SECRET_FIELD_NAME': 'PRIVATE_SECRET_VALUE'}}
        after = {'private_configuration': {'Environment': probe.hidden('PRIVATE_NEW_ENV')}}
        diagnostic = probe.ControlsChanged(before, after).diagnostic
        self.assertNotIn('PRIVATE', json.dumps(diagnostic))
        self.assertEqual([row['path'] for row in diagnostic['changed_fields']],
                         ['private_configuration.Environment', 'private_configuration.field_inventory'])

    def test_target_order_and_deployment_metadata_are_not_operating_changes(self):
        with Fixture() as f:
            f.candidate()
            f.targets[probe.CLASSIC[0]]['Targets'].reverse()
            f.live.update(RevisionId='new', LastModified='new')
            f.rules[probe.CLASSIC[0]]['ResponseMetadata'] = {'RequestId': 'new'}
            f.schedule.update(CreationDate='different', LastModificationDate='different', ResponseMetadata={'RequestId': 'new'})
            self.assertTrue(f.inspect()['baseline_compared'])

    def test_exact_receipt_zip_source_commit_and_clock_are_required(self):
        cases = [('schema', 'other', 'release_receipt_mismatch'), ('verified', False, 'release_receipt_mismatch'),
                 ('function', 'different', 'release_receipt_mismatch'), ('code_sha256', 'bad', 'release_receipt_mismatch'),
                 ('zip_bytes', True, 'release_receipt_mismatch'), ('zip_sha256_hex', 'bad', 'release_receipt_mismatch'),
                 ('commit', 'PRIVATE_BAD_COMMIT', 'receipt_commit_invalid'), ('commit', {'PRIVATE': True}, 'receipt_commit_invalid'),
                 ('deployed_at', 'PRIVATE_CLOCK', 'receipt_clock_invalid'), ('deployed_at', '2026-99-01T00:00:00Z', 'receipt_clock_invalid'),
                 ('source', [], 'release_receipt_mismatch'), ('source', {'lambda_function.py': {'sha256': 'bad', 'bytes': 1}}, 'receipt_handler_mismatch')]
        for key, value, reason in cases:
            with self.subTest(key=key, value=value), Fixture() as f:
                f.candidate()
                f.receipt[key] = value
                self.stop(f, reason)
        with Fixture() as f:
            f.candidate()
            f.git_source = b'incorrect committed handler'
            self.stop(f, 'receipt_commit_source_mismatch')
        with Fixture() as f:
            f.make_receipt()
            f.git_source = PREDECESSOR
            self.assertEqual(f.inspect()['source_phase'], 'predecessor')

    def test_candidate_checkout_bytes_are_bound_even_when_receipt_is_valid(self):
        with Fixture() as f:
            f.candidate()
            (f.source / 'lambda_function.py').write_bytes(b'newer unrelated checkout')
            self.stop(f, 'live_handler_not_reviewed')

    def test_only_exact_receipt_nosuchkey_is_optional(self):
        for code in ('AccessDenied', 'AccessDeniedException', '403', '404', 'NotFound', 'NoSuchBucket', 'InternalError', 'RequestTimeout'):
            with self.subTest(code=code), Fixture() as f:
                f.errors['get_object'] = SDKError(code)
                self.stop(f, 'access_denied_stop' if code in ('AccessDenied', 'AccessDeniedException', '403') else 'aws_read_failed_details_withheld')
        with Fixture() as f:
            f.errors['get_object'] = TimeoutError('PRIVATE_TIMEOUT')
            self.stop(f, 'aws_read_failed_details_withheld')
        for method in ('get_function', 'get_function_concurrency', 'describe_rule', 'list_targets_by_rule', 'get_schedule'):
            with self.subTest(method=method), Fixture() as f:
                f.errors[method] = SDKError('NoSuchKey')
                self.stop(f, 'aws_read_failed_details_withheld')

    def test_spoofed_nosuchkey_and_body_failure_cannot_mean_absent_receipt(self):
        class SpoofedMissing(Exception):
            response = {'Error': {'Code': 'NoSuchKey'}}
        with Fixture() as f:
            f.errors['get_object'] = SpoofedMissing('PRIVATE_ERROR')
            self.stop(f, 'aws_read_failed_details_withheld')
        class FailedBody:
            closed = False
            def read(self, limit):
                raise SDKError('NoSuchKey')
            def close(self):
                self.closed = True
        for candidate in (False, True):
            with self.subTest(candidate=candidate), Fixture() as f:
                if candidate:
                    f.candidate()
                body = FailedBody()
                def get_object(**kwargs):
                    f.record('get_object', kwargs)
                    return {'Body': body}
                with patch.object(f, 'get_object', get_object):
                    with self.assertRaises(SDKError):
                        f.inspect()
                self.assertTrue(body.closed)

    def test_api_byte_bounds_and_scope_allowlist(self):
        reader = probe.Reader()
        reader.calls = probe.MAX_CALLS
        with Fixture() as f:
            with self.assertRaisesRegex(probe.Stop, '^api_bound_reached$'):
                reader.read(f.get_function, FunctionName=probe.FUNCTION)
            for method, kwargs in ((f.get_object, {'Bucket': probe.BUCKET, 'Key': 'data/_state/private.json'}),
                                   (f.get_function, {'FunctionName': 'other-lambda'}),
                                   (f.describe_rule, {'Name': 'unknown'}),
                                   (f.get_schedule, {'Name': probe.SCHEDULER, 'GroupName': 'other-group'})):
                with self.assertRaisesRegex(probe.Stop, '^read_scope_not_allowed$'):
                    probe.Reader().read(method, **kwargs)
            self.assertEqual(f.calls, [])
        stream = io.BytesIO(b'x' * 11)
        with self.assertRaisesRegex(probe.Stop, '^byte_bound_reached$'):
            probe.bounded(stream, 10)
        self.assertTrue(stream.closed)
        with Fixture() as f, patch.object(probe, 'PACKAGE_BOUND', len(f.raw) - 1):
            self.stop(f, 'byte_bound_reached')

    def test_main_withholds_transport_json_and_body_failure_details(self):
        drift = probe.ControlsChanged({'private_configuration': {'Environment': probe.hidden('PRIVATE_OLD')}},
                                      {'private_configuration': {'Environment': probe.hidden('PRIVATE_NEW')}})
        for failure in (drift, TimeoutError('PRIVATE_TIMEOUT'), json.JSONDecodeError('PRIVATE_JSON', 'PRIVATE_DOC', 0),
                        OSError('PRIVATE_BODY_FAILURE'), SDKError('NoSuchKey')):
            with self.subTest(failure=type(failure).__name__):
                class Report:
                    def __init__(self):
                        self.logs = []
                        self.rows = []
                    def log(self, value):
                        self.logs.append(value)
                    def kv(self, **values):
                        self.rows.append(values)
                report = Report()
                @contextmanager
                def report_context(_):
                    yield report
                modules = {'boto3': types.SimpleNamespace(client=lambda *args, **kwargs: None),
                           'botocore.config': types.SimpleNamespace(Config=lambda **kwargs: None),
                           'botocore.exceptions': types.SimpleNamespace(ClientError=SDKError),
                           'ops_report': types.SimpleNamespace(report=report_context)}
                with patch.dict(probe.os.environ, {'GITHUB_ACTIONS': 'true'}), \
                     patch.dict(probe.sys.modules, modules), \
                     patch.object(probe.signal, 'signal'), patch.object(probe.signal, 'alarm') as alarm, \
                     patch.object(probe, 'inspect', side_effect=failure):
                    with self.assertRaises(SystemExit) as stopped:
                        probe.main()
                self.assertEqual(stopped.exception.code, 1)
                self.assertFalse(report.rows[-1]['completed'])
                if failure is drift:
                    self.assertEqual(report.rows[-1]['stop_reason'], 'operating_controls_changed')
                    self.assertEqual(report.logs, ['CONTROL_DIFF_JSON ' + json.dumps(drift.diagnostic, sort_keys=True)])
                else:
                    self.assertEqual(report.rows[-1]['stop_reason'], 'unexpected_failure_details_withheld')
                    self.assertEqual(report.logs, [])
                self.assertNotIn('PRIVATE', json.dumps(report.logs + report.rows))
                alarm.assert_called_with(0)

    def test_git_error_details_and_runner_only_boundary(self):
        with Fixture() as f, patch.object(probe.subprocess, 'check_output', side_effect=RuntimeError('PRIVATE_GIT_ERROR')):
            self.stop(f, 'commit_source_unavailable')
        with patch.dict(probe.os.environ, {'GITHUB_ACTIONS': 'false'}):
            with self.assertRaisesRegex(probe.Stop, '^runner_only$'):
                probe.main()


if __name__ == '__main__':
    unittest.main()
