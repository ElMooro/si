"""Invented metadata/streams and offline SDK exceptions; no AWS or archives."""
import base64
import copy
from contextlib import redirect_stdout
import importlib.util
import io
import json
import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

from botocore.exceptions import ClientError, ParamValidationError

ROOT = Path(__file__).resolve().parents[2]
spec = importlib.util.spec_from_file_location('receipt_diag', ROOT / 'aws/ops/staged/ops_6471_series_receipt_provenance.py')
p = importlib.util.module_from_spec(spec)
spec.loader.exec_module(p)
CANARY = 'PRIVATE_CREDENTIAL_ENV_SOURCE_URL_ERROR_CANARY'


def sdk_error(code, status):
    return ClientError({'Error': {'Code': code, 'Message': CANARY},
        'ResponseMetadata': {'HTTPStatusCode': status}}, 'GetObject')


class Body(io.BytesIO):
    read_error = None
    close_error = None
    def read(self, count):
        if self.read_error:
            raise self.read_error
        return super().read(count)
    def close(self):
        super().close()
        if self.close_error:
            raise self.close_error


class Fixture:
    def __init__(self):
        self.calls = []
        self.function_error = self.receipt_error = None
        self.function_status = self.receipt_status = 200
        self.cfg = {'FunctionName': p.FUNCTION,
            'FunctionArn': 'arn:aws:lambda:us-east-1:857687956942:function:' + p.FUNCTION,
            'CodeSha256': p.RETAINED_CODE_SHA, 'CodeSize': 1077860,
            'State': 'Active', 'LastUpdateStatus': 'Successful',
            'Environment': {'Variables': {'secret': CANARY}}, 'Unknown': CANARY}
        self.receipt = {'schema': 'release-receipt.v1', 'function': p.FUNCTION,
            'commit': p.RETAINED_RELEASE_COMMIT, 'run_id': p.RETAINED_RUN,
            'code_sha256': p.RETAINED_CODE_SHA, 'zip_sha256_hex': p.RETAINED_ZIP_SHA,
            'zip_bytes': 1077860, 'verified': True,
            'source': {'lambda_function.py': {'sha256': p.RETAINED_HANDLER_SHA, 'bytes': 38171},
                       CANARY: {'source': CANARY}}, 'actor': CANARY, 'Unknown': CANARY}
        self.raw_override = None
        self.body = None
        self.body_read_error = self.body_close_error = None
        self.evidence = p.initial_evidence()
        self.reader = p.Reader(self.evidence, ClientError, (TimeoutError,))

    def get_function(self, **kwargs):
        self.calls.append(('get_function', kwargs))
        if self.function_error:
            raise self.function_error
        return {'Configuration': copy.deepcopy(self.cfg), 'Code': {'Location': CANARY},
                'ResponseMetadata': {'HTTPStatusCode': self.function_status}}

    def get_object(self, **kwargs):
        self.calls.append(('get_object', kwargs))
        if self.receipt_error:
            raise self.receipt_error
        raw = self.raw_override if self.raw_override is not None else json.dumps(self.receipt).encode()
        self.body = Body(raw)
        self.body.read_error, self.body.close_error = self.body_read_error, self.body_close_error
        return {'Body': self.body, 'ResponseMetadata': {'HTTPStatusCode': self.receipt_status}}

    def inspect(self):
        return p.inspect(self, self, self.reader)


class Diagnostics(unittest.TestCase):
    def unqualified(self, result):
        for key in ['baseline_qualified', 'release_qualified', 'native_execution_role_LIST_proven',
                    'live_ZIP_or_repository_source_verified', 'atomic_snapshot_proven']:
            self.assertIs(result[key], False)
        self.assertEqual(result['aws_writes'], 0)
        self.assertEqual(result['producer_invokes'], 0)
        self.assertNotIn(CANARY, json.dumps(result))

    def stop(self, f, reason):
        with self.assertRaisesRegex(p.Stop, '^' + reason + '$'):
            f.inspect()
        self.unqualified(f.evidence)

    def test_retained_receipt_and_exact_two_reads_are_observational(self):
        f = Fixture(); result = f.inspect(); self.unqualified(result)
        self.assertTrue(f.body.closed)
        self.assertFalse(result['comparisons']['original_6470_commit_matches'])
        self.assertTrue(result['comparisons']['retained_release_commit_and_run_match'])
        self.assertTrue(result['comparisons']['receipt_matches_current_function_hash_and_size'])
        self.assertEqual(f.calls, [('get_function', {'FunctionName': p.FUNCTION}),
            ('get_object', {'Bucket': p.BUCKET, 'Key': p.KEY})])
        self.assertEqual(f.reader.calls, 2)

    def test_other_commits_and_changed_packages_never_refresh_pins(self):
        for commit in [p.ORIGINAL_EXPECTED_COMMIT, 'a' * 40]:
            with self.subTest(commit=commit):
                f = Fixture(); f.receipt['commit'] = commit; f.receipt['run_id'] = '123'; result = f.inspect()
                self.unqualified(result); self.assertFalse(result['comparisons']['retained_release_commit_and_run_match'])
        f = Fixture(); f.cfg['CodeSha256'] = base64.b64encode(b'z' * 32).decode()
        result = f.inspect(); self.assertFalse(result['comparisons']['receipt_matches_current_function_hash_and_size'])
        self.unqualified(result); self.assertEqual(p.ORIGINAL_EXPECTED_COMMIT, '7ab550f69d0653d1ad2a63cb65407aa7c0ae663c')

    def test_typed_missing_is_distinct_from_access_transport_and_other_errors(self):
        f = Fixture(); f.receipt_error = sdk_error('NoSuchKey', 404); result = f.inspect()
        self.assertFalse(result['receipt_present']); self.assertIsNone(result['actual_receipt'])
        self.assertEqual(result['read_outcomes']['get_object']['outcome'], 'typed_NoSuchKey'); self.unqualified(result)
        for position in ['function_error', 'receipt_error']:
            for error, reason in [(sdk_error('AccessDenied', 403), 'access_denied'),
                    (sdk_error(CANARY, 500), 'client_error_details_withheld'),
                    (TimeoutError(CANARY), 'transport_error_details_withheld'),
                    (ValueError(CANARY), 'unexpected_exception_details_withheld'),
                    (TypeError(CANARY), 'unexpected_exception_details_withheld'),
                    (ParamValidationError(report=CANARY), 'unexpected_exception_details_withheld'),
                    (p.Stop(CANARY), 'unexpected_exception_details_withheld')]:
                with self.subTest(position=position, error=type(error).__name__, reason=reason):
                    f = Fixture(); setattr(f, position, error); self.stop(f, reason)
                    self.assertEqual(f.reader.calls, 1 if position == 'function_error' else 2)
        for status in [None, True, 404.0, 403]:
            f = Fixture(); f.receipt_error = sdk_error('NoSuchKey', status); self.stop(f, 'client_error_details_withheld')

    def test_strict_HTTP_at_both_positions_and_stream_closed_on_refusal(self):
        for attr in ['function_status', 'receipt_status']:
            for value in [None, True, 200.0, '200', 403, 500]:
                with self.subTest(attr=attr, value=value):
                    f = Fixture(); setattr(f, attr, value); self.stop(f, 'response_HTTP_not_successful')
                    if f.body: self.assertTrue(f.body.closed)

    def test_function_identity_readiness_and_types_are_required(self):
        for key, value in [('FunctionName', CANARY), ('FunctionArn', CANARY), ('State', 'Pending'),
                          ('LastUpdateStatus', 'Failed'), ('CodeSize', True), ('CodeSize', 0),
                          ('CodeSize', 1.1), ('CodeSha256', CANARY), ('CodeSha256', 'a' * 44)]:
            with self.subTest(key=key, value=value):
                f = Fixture(); f.cfg[key] = value; self.stop(f, 'function_metadata_unqualified')
                self.assertEqual(f.reader.calls, 1); self.assertIsNone(f.evidence['actual_function'])

    def test_receipt_safe_partial_fields_survive_malformed_metadata_without_echo(self):
        for key, value in [('schema', CANARY), ('function', CANARY), ('commit', CANARY),
                          ('run_id', CANARY), ('run_id', True), ('verified', 1), ('verified', False),
                          ('code_sha256', CANARY), ('zip_sha256_hex', CANARY), ('zip_bytes', True),
                          ('zip_bytes', 0), ('zip_bytes', 1.5), ('source', CANARY)]:
            with self.subTest(key=key, value=value):
                f = Fixture(); f.receipt[key] = value; self.stop(f, 'receipt_metadata_unqualified')
                self.assertTrue(f.body.closed)
                if key != 'commit': self.assertEqual(f.evidence['actual_receipt']['commit'], p.RETAINED_RELEASE_COMMIT)
        f = Fixture(); f.receipt['source']['lambda_function.py']['sha256'] = CANARY
        self.stop(f, 'receipt_metadata_unqualified')

    def test_JSON_shape_duplicates_nonfinite_invalid_and_bounds(self):
        for raw, reason in [(b'{', 'receipt_JSON_invalid'), (CANARY.encode(), 'receipt_JSON_invalid'),
                (b'{"unknown":NaN}', 'receipt_JSON_invalid'), (b'{"unknown":Infinity}', 'receipt_JSON_invalid'),
                (b'{"private":1,"private":2}', 'receipt_JSON_invalid'),
                (b'[]', 'receipt_shape_unqualified'), (b'null', 'receipt_shape_unqualified'),
                (b'x' * (p.BYTE_BOUND + 1), 'receipt_byte_bound_or_type')]:
            with self.subTest(reason=reason):
                f = Fixture(); f.raw_override = raw; self.stop(f, reason); self.assertTrue(f.body.closed)

    def test_stream_failures_and_deadline_are_fatal_and_sanitized(self):
        for attr, reason in [('body_read_error', 'receipt_stream_read_failed'),
                             ('body_close_error', 'receipt_stream_close_failed')]:
            f = Fixture(); setattr(f, attr, ValueError(CANARY)); self.stop(f, reason); self.assertTrue(f.body.closed)
        for attr in ['function_error', 'receipt_error', 'body_read_error', 'body_close_error']:
            f = Fixture(); setattr(f, attr, p.Deadline()); self.stop(f, 'diagnostic_time_bound')

    def test_scope_count_and_malformed_SDK_responses(self):
        f = Fixture(); f.inspect()
        with self.assertRaisesRegex(p.Stop, '^read_count_bound$'):
            f.reader.read(f.get_object, Bucket=p.BUCKET, Key=p.KEY)
        for kwargs in [{'Bucket': p.BUCKET, 'Key': 'other'}, {'Bucket': 'other', 'Key': p.KEY},
                       {'Bucket': p.BUCKET, 'Key': p.KEY, 'VersionId': 'x'}]:
            f = Fixture()
            with self.assertRaisesRegex(p.Stop, '^read_scope_not_allowed$'): f.reader.read(f.get_object, **kwargs)
            self.assertEqual(f.calls, [])
        for value in [None, [], CANARY]:
            f = Fixture()
            def get_object(**kwargs): return value
            with self.assertRaisesRegex(p.Stop, '^malformed_SDK_response$'):
                f.reader.read(get_object, Bucket=p.BUCKET, Key=p.KEY)
            self.unqualified(f.evidence)

    def test_client_setup_programming_errors_and_untrusted_Stop_are_sanitized(self):
        import boto3
        for error in [ValueError(CANARY), p.Stop(CANARY)]:
            with tempfile.TemporaryDirectory(prefix='receipt-offline-') as td:
                stdout = io.StringIO()
                with patch.dict(os.environ, {'GITHUB_ACTIONS': 'true', 'GITHUB_WORKSPACE': td,
                                             'GITHUB_STEP_SUMMARY': ''}), patch.object(boto3, 'client', side_effect=error), \
                     patch.object(p.signal, 'signal'), patch.object(p.signal, 'alarm'), redirect_stdout(stdout):
                    with self.assertRaises(SystemExit) as cm: p.main()
                    self.assertEqual(cm.exception.code, 1)
                text = Path(td, 'aws/ops/reports/latest/ops_6471_series_receipt_provenance.md').read_text(encoding='utf-8')
                self.assertIn('**Status:** failure', text)
                self.assertIn('unexpected_diagnostic_failure_details_withheld', text)
                self.assertIn('| 0 | False | unexpected_diagnostic_failure_details_withheld |', text)
                self.assertNotIn(CANARY, text + stdout.getvalue())
                self.assertNotIn('TECHNICAL_JSON ', text)

    def test_actual_main_report_success_and_failure_preserve_red_semantics(self):
        import boto3
        sys.path.insert(0, str(ROOT / 'aws/ops'))
        for error in [None, sdk_error('NoSuchKey', 404), sdk_error('AccessDenied', 403), ValueError(CANARY)]:
            with tempfile.TemporaryDirectory(prefix='receipt-offline-') as td:
                f = Fixture(); f.receipt_error = error; stdout = io.StringIO()
                with patch.dict(os.environ, {'GITHUB_ACTIONS': 'true', 'GITHUB_WORKSPACE': td,
                                             'GITHUB_STEP_SUMMARY': ''}), patch.object(boto3, 'client', return_value=f), \
                     patch.object(p.signal, 'signal'), patch.object(p.signal, 'alarm') as alarm, redirect_stdout(stdout):
                    if error is None or (isinstance(error, ClientError) and error.response['Error']['Code'] == 'NoSuchKey'):
                        p.main(); failed = False
                    else:
                        with self.assertRaises(SystemExit) as cm: p.main()
                        self.assertEqual(cm.exception.code, 1); failed = True
                text = Path(td, 'aws/ops/reports/latest/ops_6471_series_receipt_provenance.md').read_text(encoding='utf-8')
                self.assertIn('**Status:** ' + ('failure' if failed else 'success'), text)
                self.assertIn('RECEIPT_PROVENANCE_JSON ', text); self.assertNotIn('TECHNICAL_JSON ', text)
                self.assertNotIn(CANARY, text + stdout.getvalue()); self.assertIn('"release_qualified": false', text)
                self.assertEqual(alarm.call_args_list[-1].args, (0,)); self.assertEqual(alarm.call_args_list[0].args, (45,))


if __name__ == '__main__':
    unittest.main()
