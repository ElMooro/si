"""Invented SDK/package fixtures only. No AWS, archives or network access."""
from contextlib import ExitStack
import copy
import hashlib
import importlib.util
import io
import json
from pathlib import Path
import unittest
from unittest.mock import patch
import zipfile

ROOT = Path(__file__).resolve().parents[2]

def load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module

old = load('historical_fixture', ROOT / 'tests/ops/test_series_admission_acceptance.py')
p = load('prospective', ROOT / 'aws/ops/staged/ops_6470_series_reliability_prospective_controls.py')
ARN = 'arn:aws:lambda:us-east-1::runtime:' + 'a' * 64


class Fixture(old.Fixture):
    def __enter__(self):
        super().__enter__()
        historical = super().inspect()
        self.historical = self.root / 'historical.json'
        self.historical.write_text(json.dumps(historical), encoding='utf-8')
        self.source.joinpath('lambda_function.py').write_bytes(old.PREDECESSOR)
        self.make_receipt()
        self.git_source = old.PREDECESSOR
        self.live['RuntimeVersionConfig'] = {'RuntimeVersionArn': ARN}
        self.management = {'FunctionArn': self.live['FunctionArn'], 'UpdateRuntimeOn': 'Auto',
                           'ResponseMetadata': {'HTTPStatusCode': 200}, 'RuntimeVersionArn': None}
        self.simulated = {'EvaluationResults': [{'EvalActionName': 's3:ListBucket',
            'EvalResourceName': 'arn:aws:s3:::' + p.BUCKET, 'EvalDecision': 'allowed',
            'MissingContextValues': [], 'MatchedStatements': [{'SourcePolicyId': 'PRIVATE_POLICY'}]}],
            'IsTruncated': False, 'ResponseMetadata': {'HTTPStatusCode': 200}}
        self.calls.clear(); self.signed_gets.clear()
        self.stack.enter_context(patch.object(p, 'ROOT', self.root))
        self.stack.enter_context(patch.object(p, 'BASELINE', self.baseline))
        self.stack.enter_context(patch.object(p, 'HISTORICAL_BASELINE', self.historical))
        self.stack.enter_context(patch.object(p, 'HISTORICAL_SHA256', hashlib.sha256(self.historical.read_bytes()).hexdigest()))
        self.stack.enter_context(patch.object(p, 'PREDECESSOR_COMMIT', old.probe.PREDECESSOR_COMMIT))
        self.stack.enter_context(patch.object(p, 'PREDECESSOR_SHA256', hashlib.sha256(old.PREDECESSOR).hexdigest()))
        self.stack.enter_context(patch.object(p, 'INSTALLED_CODE_SHA256', self.live['CodeSha256']))
        self.stack.enter_context(patch.object(p, 'INSTALLED_RELEASE', old.RECEIPT_COMMIT))
        self.stack.enter_context(patch.object(p, 'package_source_evidence', return_value={'invented': True}))
        return self

    def get_function(self, **kwargs):
        return copy.deepcopy(super().get_function(**kwargs))

    def get_runtime_management_config(self, **kwargs):
        self.record('get_runtime_management_config', kwargs)
        return copy.deepcopy(self.management)

    def simulate_principal_policy(self, **kwargs):
        self.record('simulate_principal_policy', kwargs)
        return copy.deepcopy(self.simulated)

    def git_show(self, command, **kwargs):
        if command == ['git', 'rev-parse', 'HEAD']:
            return b'c' * 40 + b'\n'
        if command[:2] == ['git', 'show'] and not command[2].startswith(old.probe.PREDECESSOR_COMMIT + ':'):
            return self.git_source
        return super().git_show(command, **kwargs)

    def inspect(self):
        return p.inspect(self, self, self, self, self, p.Reader(old.SDKError, (TimeoutError,)), opener=self.opener)


class ProspectiveTests(unittest.TestCase):
    def stop(self, fixture, reason):
        with self.assertRaisesRegex(p.Stop, '^' + reason + '$'):
            fixture.inspect()

    def test_capture_binds_new_contract_full_runtime_and_distinct_mode_without_private_values(self):
        with Fixture() as f:
            result = f.inspect()
            runtime = result['operating_controls']['runtime_evidence']
            self.assertEqual(runtime['runtime_version_config'], {'RuntimeVersionArn': ARN})
            self.assertEqual(runtime['runtime_management']['UpdateRuntimeOn'], 'Auto')
            self.assertEqual(runtime['runtime_management']['RuntimeVersionArn'], None)
            self.assertFalse(runtime['runtime_version_shape']['error_field_present'])
            self.assertEqual(result['schema'], 'series-reliability-prospective-baseline.v1')
            self.assertEqual(result['receipt_commit'], old.RECEIPT_COMMIT)
            self.assertEqual(result['historical_PR75']['status'], 'failed_not_recovered')
            self.assertEqual(result['historical_PR75']['old_runtime_identity_mode_and_cause'], 'unavailable')
            self.assertEqual(result['historical_PR75']['baseline_sha256'], hashlib.sha256(f.historical.read_bytes()).hexdigest())
            self.assertFalse(result['release_qualified'])
            self.assertFalse(result['LIST_permission']['native_execution_role_LIST_proven'])
            self.assertTrue(result['LIST_permission']['all_identity_simulations_allowed'])
            self.assertEqual(result['aws_read_calls'], 21)
            self.assertNotIn('PRIVATE', json.dumps(result))
            self.assertEqual(result['aws_writes'], 0)
            self.assertEqual(result['producer_invokes'], 0)
            self.assertEqual(f.signed_gets, [('PRIVATE_SIGNED_URL', 30)])
            self.assertEqual([name for name, _ in f.calls].count('get_object'), 1)
            self.assertEqual([name for name, _ in f.calls].count('simulate_principal_policy'), 4)
            self.assertNotIn('list_objects_v2', [name for name, _ in f.calls])
            self.assertEqual([kw for name, kw in f.calls if name == 'get_object'],
                             [{'Bucket': p.BUCKET, 'Key': p.RECEIPT_KEY}])

    def test_no_recapture_existing_prospective_baseline(self):
        with Fixture() as f:
            f.baseline.write_text('{}', encoding='utf-8')
            self.stop(f, 'prospective_baseline_already_exists_no_recapture')
            self.assertEqual(f.calls, [])

    def test_capture_only_exact_installed_handler_package_and_receipt(self):
        for change, reason in (
            (lambda f: f.source.joinpath('lambda_function.py').write_bytes(old.CANDIDATE), 'installed_handler_not_exact_reviewed_PR75'),
            (lambda f: setattr(p, 'INSTALLED_CODE_SHA256', 'changed'), 'installed_package_changed'),
            (lambda f: f.receipt.update(commit='b' * 40), 'receipt_commit_source_mismatch'),
            (lambda f: setattr(f, 'receipt', None), 'installed_receipt_not_exact')):
            with self.subTest(reason=reason), Fixture() as f:
                if reason == 'receipt_commit_source_mismatch':
                    f.git_source = old.CANDIDATE
                change(f)
                self.stop(f, reason)

    def test_every_other_historical_control_stays_exact(self):
        for change in (
            lambda f: f.live['Environment']['Variables'].update(PRIVATE_CHANGED='PRIVATE_VALUE'),
            lambda f: f.targets[p.CLASSIC[1]]['Targets'][0].update(Input='PRIVATE_CHANGED'),
            lambda f: f.schedule.update(ScheduleExpression='rate(11 minutes)'),
            lambda f: f.live.update(LoggingConfig={'LogGroup': 'PRIVATE_CHANGED'})):
            with self.subTest(change=change), Fixture() as f:
                change(f)
                self.stop(f, 'unapproved_historical_control_difference')

    def test_runtime_error_absence_shape_and_unknown_fields_cannot_be_replaced_by_digest(self):
        for value in (None, {}, {'RuntimeVersionArn': ARN, 'Error': {'ErrorCode': 'PRIVATE_ERROR', 'Message': 'PRIVATE_DETAIL'}},
                      {'RuntimeVersionArn': ARN, 'Unknown': 'PRIVATE'}, {'RuntimeVersionArn': 'PRIVATE_INVALID'}):
            with self.subTest(value=value), Fixture() as f:
                f.live['RuntimeVersionConfig'] = value
                self.stop(f, 'runtime_version_not_qualifiable')

    def test_distinct_management_mode_and_manual_arn_are_retained(self):
        for mode in ('Auto', 'FunctionUpdate', 'Manual'):
            with self.subTest(mode=mode), Fixture() as f:
                f.management['UpdateRuntimeOn'] = mode
                f.management['RuntimeVersionArn'] = ARN if mode == 'Manual' else None
                result = f.inspect()
                self.assertEqual(result['operating_controls']['runtime_evidence']['runtime_management']['UpdateRuntimeOn'], mode)

    def test_invalid_management_shapes_modes_and_manual_arn_refuse(self):
        for change, reason in (
            (lambda f: f.management.update(UpdateRuntimeOn='PRIVATE_UNKNOWN'), 'runtime_management_not_qualifiable'),
            (lambda f: f.management.update(Unknown='PRIVATE'), 'runtime_management_not_qualifiable'),
            (lambda f: f.management.update(FunctionArn='PRIVATE_OTHER'), 'runtime_management_not_qualifiable'),
            (lambda f: f.management.update(UpdateRuntimeOn='Manual'), 'runtime_manual_arn_mismatch'),
            (lambda f: f.management.update(RuntimeVersionArn=ARN), 'runtime_management_arn_unexpected'),
            (lambda f: f.management.update(ResponseMetadata={'HTTPStatusCode': 500}), 'runtime_management_response_not_successful'),
            (lambda f: f.management.update(ResponseMetadata={'HTTPStatusCode': 200.0}), 'runtime_management_response_not_successful')):
            with self.subTest(reason=reason), Fixture() as f:
                change(f); self.stop(f, reason)

    def test_late_runtime_package_or_control_change_refuses(self):
        for change in (
            lambda f: f.live['RuntimeVersionConfig'].update(RuntimeVersionArn=ARN[:-1] + 'b'),
            lambda f: f.live.update(CodeSha256='changed'),
            lambda f: f.live.update(CodeSize=123),
            lambda f: f.live.update(FunctionName='PRIVATE_OTHER'),
            lambda f: f.live.update(FunctionArn='PRIVATE_OTHER'),
            lambda f: f.live.update(LoggingConfig={'LogGroup': 'PRIVATE_CHANGED'})):
            with self.subTest(change=change), Fixture() as f:
                original = f.get_function
                def mutate(**kwargs):
                    if any(name == 'get_function' for name, _ in f.calls):
                        change(f)
                    return original(**kwargs)
                with patch.object(f, 'get_function', mutate):
                    mutate.__name__ = 'get_function'
                    self.stop(f, 'capture_changed_between_observations')

    def test_simulation_denial_missing_context_or_boundary_denial_never_qualifies(self):
        for update in ({'EvalDecision': 'implicitDeny'}, {'EvalDecision': 'explicitDeny'},
                       {'MissingContextValues': ['PRIVATE_CONTEXT']},
                       {'PermissionsBoundaryDecisionDetail': {'AllowedByPermissionsBoundary': False}},
                       {'OrganizationsDecisionDetail': {'AllowedByOrganizations': False}}):
            with self.subTest(update=update), Fixture() as f:
                f.simulated['EvaluationResults'][0].update(update)
                result = f.inspect()
                self.assertFalse(result['LIST_permission']['all_identity_simulations_allowed'])
                self.assertFalse(result['release_qualified'])
                self.assertNotIn('PRIVATE', json.dumps(result))

    def test_simulation_response_count_pagination_action_resource_and_decision_are_bounded(self):
        for update in ({'ResponseMetadata': None}, {'ResponseMetadata': {'HTTPStatusCode': 500}},
                       {'ResponseMetadata': {'HTTPStatusCode': 200.0}}, {'IsTruncated': True}, {'Marker': 'PRIVATE'}, {'EvaluationResults': []},
                       {'EvaluationResults': [{'EvalActionName': 's3:ListBucket', 'EvalResourceName': 'arn:aws:s3:::' + p.BUCKET, 'EvalDecision': 'allowed', 'PermissionsBoundaryDecisionDetail': {'AllowedByPermissionsBoundary': 'PRIVATE'}}]},
                       {'EvaluationResults': [{'EvalActionName': 's3:GetObject', 'EvalResourceName': 'arn:aws:s3:::' + p.BUCKET, 'EvalDecision': 'allowed'}]},
                       {'EvaluationResults': [{'EvalActionName': 's3:ListBucket', 'EvalResourceName': 'PRIVATE', 'EvalDecision': 'allowed'}]}):
            with self.subTest(update=update), Fixture() as f:
                f.simulated.update(update); self.stop(f, 'list_simulation_response_unqualified')

    def test_allowlist_rejects_arbitrary_principal_action_prefix_or_context(self):
        for change in (lambda k: k.update(PolicySourceArn='PRIVATE'), lambda k: k.update(ActionNames=['s3:GetObject']),
                       lambda k: k['ContextEntries'][0].update(ContextKeyValues=['data/private/']),
                       lambda k: k.update(PolicyInputList=['PRIVATE_ADDED_POLICY'])):
            with self.subTest(change=change), Fixture() as f:
                kwargs = copy.deepcopy(p.simulation_requests()[0]); change(kwargs)
                with self.assertRaisesRegex(p.Stop, 'read_scope_not_allowed'):
                    p.Reader(old.SDKError).read(f.simulate_principal_policy, **kwargs)
                self.assertEqual(f.calls, [])

    def test_access_or_transport_error_is_sanitized_and_stops(self):
        for error, reason in ((old.SDKError('AccessDenied'), 'access_denied_stop'),
                              (TimeoutError('PRIVATE'), 'aws_read_failed_details_withheld')):
            with self.subTest(reason=reason), Fixture() as f:
                f.errors['get_runtime_management_config'] = error
                self.stop(f, reason)

    def test_only_optional_iam_access_or_transport_failure_preserves_the_new_capture(self):
        for error, reason in ((old.SDKError('AccessDenied'), 'access_denied_stop'),
                              (TimeoutError('PRIVATE'), 'aws_read_failed_details_withheld')):
            with self.subTest(reason=reason), Fixture() as f:
                f.errors['simulate_principal_policy'] = error
                result = f.inspect()
                self.assertEqual(result['LIST_permission']['stop_reason'], reason)
                self.assertFalse(result['LIST_permission']['modeled_evidence_available'])
                self.assertFalse(result['LIST_permission']['native_execution_role_LIST_proven'])
                self.assertFalse(result['release_qualified'])
                self.assertEqual(result['aws_read_calls'], 18)
                self.assertEqual(result['operating_controls']['runtime_evidence']['runtime_version_config']['RuntimeVersionArn'], ARN)
                self.assertNotIn('PRIVATE', json.dumps(result))
        from botocore.exceptions import ParamValidationError
        for error in (ValueError('PRIVATE'), TypeError('PRIVATE'), ParamValidationError(report='PRIVATE')):
            with self.subTest(method_error=type(error).__name__), Fixture() as f:
                f.errors['simulate_principal_policy'] = error
                self.stop(f, 'unexpected_aws_reader_failure_details_withheld')
        for error in (p.Stop('api_bound_reached'), p.Stop('read_scope_not_allowed'), ValueError('PRIVATE')):
            with self.subTest(error=error), Fixture() as f:
                with patch.object(p, 'simulation_evidence', side_effect=error):
                    with self.assertRaises(type(error)):
                        f.inspect()

    def test_full_package_repository_python_sources_are_checked_without_source_disclosure(self):
        # Test the real source check on invented ZIP/git bytes.
        source = ROOT / 'aws/lambdas' / p.FUNCTION / 'source/lambda_function.py'
        module_path = 'aws/shared/invented_helper.py'
        source_path = str(source.relative_to(ROOT))
        stream = io.BytesIO()
        with zipfile.ZipFile(stream, 'w') as archive:
            archive.writestr('lambda_function.py', b'handler')
            archive.writestr('invented_helper.py', b'helper')
        def git_read(command, **kwargs):
            if command[:2] == ['git', 'ls-tree']:
                return (module_path + '\n' + source_path + '\n').encode()
            return b'handler' if command[-1].endswith(source_path) else b'helper'
        with patch.object(p.subprocess, 'check_output', git_read):
            result = p.package_source_evidence(stream.getvalue(), 'f' * 40, source)
            self.assertEqual(result['repository_python_members_verified'], 2)
            bad = io.BytesIO()
            with zipfile.ZipFile(bad, 'w') as archive:
                archive.writestr('lambda_function.py', b'handler')
                archive.writestr('invented_helper.py', b'PRIVATE_WRONG')
            with self.assertRaisesRegex(p.Stop, 'package_repository_source_mismatch'):
                p.package_source_evidence(bad.getvalue(), 'f' * 40, source)


if __name__ == '__main__':
    unittest.main()
