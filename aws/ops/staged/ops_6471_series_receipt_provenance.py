"""Two-read receipt diagnostics only; never baseline or release acceptance."""
import base64
import json
import os
from pathlib import Path
import re
import signal
import sys

ROOT = Path(__file__).resolve().parents[3]
FUNCTION = 'justhodl-series-extractor'
BUCKET = 'justhodl-dashboard-live'
KEY = 'data/ops/releases/' + FUNCTION + '.json'
ORIGINAL_EXPECTED_COMMIT = '7ab550f69d0653d1ad2a63cb65407aa7c0ae663c'
RETAINED_RELEASE_COMMIT = 'e5a4c11cd44fc103ae64a0ca43a7161c31ec7808'
RETAINED_RUN = '36982007891'
RETAINED_CODE_SHA = 'w1YX8/Ni49DJkBRIUpoksNzABPkd/rF+d86eX0LtXtY='
RETAINED_ZIP_SHA = 'c35617f3f362e3d0c9901448529a24b0dcc004f91dfeb17e77ce9e5f42ed5ed6'
RETAINED_HANDLER_SHA = 'bcd80ce358aa433d9de33c45b9fb2900987c63046d343462b3c359b7c3724867'
BYTE_BOUND = 1024 * 1024
MISSING = object()


class Stop(Exception):
    pass


class Deadline(Stop):
    def __init__(self):
        super().__init__('diagnostic_time_bound')


def require(condition, reason):
    if not condition:
        raise Stop(reason)


def initial_evidence():
    return {'purpose': 'receipt provenance diagnostics only',
        'original_6470_expected': {'function': FUNCTION, 'commit': ORIGINAL_EXPECTED_COMMIT},
        'retained_PR75_release': {'commit': RETAINED_RELEASE_COMMIT, 'run_id': RETAINED_RUN,
            'code_sha256': RETAINED_CODE_SHA, 'zip_sha256_hex': RETAINED_ZIP_SHA,
            'zip_bytes': 1077860, 'handler_sha256': RETAINED_HANDLER_SHA, 'handler_bytes': 38171},
        'read_outcomes': {}, 'actual_function': None, 'actual_receipt': None,
        'baseline_qualified': False, 'release_qualified': False,
        'native_execution_role_LIST_proven': False, 'aws_writes': 0, 'producer_invokes': 0,
        'live_ZIP_or_repository_source_verified': False,
        'atomic_snapshot_proven': False}


class Reader:
    def __init__(self, evidence, client_error_type=(), transport_types=()):
        self.evidence = evidence
        self.client_error_type = client_error_type
        self.transport_types = transport_types
        self.calls = 0

    def read(self, method, **kwargs):
        name = method.__name__
        allowed = {'get_function': {'FunctionName': FUNCTION},
                   'get_object': {'Bucket': BUCKET, 'Key': KEY}}
        require(name in allowed and kwargs == allowed[name], 'read_scope_not_allowed')
        require(self.calls < 2, 'read_count_bound')
        self.calls += 1
        row = self.evidence['read_outcomes'][name] = {'outcome': 'request_started'}
        try:
            result = method(**kwargs)
        except Exception as exc:
            if isinstance(exc, Deadline):
                raise
            if isinstance(exc, self.client_error_type):
                response = getattr(exc, 'response', None)
                error = response.get('Error', {}) if type(response) is dict else {}
                code = error.get('Code') if type(error) is dict else None
                metadata = response.get('ResponseMetadata', {}) if type(response) is dict else {}
                status = metadata.get('HTTPStatusCode') if type(metadata) is dict else None
                row['http_status'] = status if type(status) is int and 100 <= status <= 599 else None
                if name == 'get_object' and code == 'NoSuchKey' and type(status) is int and status == 404:
                    row['outcome'] = 'typed_NoSuchKey'
                    return MISSING
                row['outcome'] = ('access_denied' if code in
                    ('AccessDenied', 'AccessDeniedException', 'UnauthorizedOperation', '403')
                    else 'client_error_details_withheld')
            elif isinstance(exc, self.transport_types):
                row['outcome'] = 'transport_error_details_withheld'
            else:
                row['outcome'] = 'unexpected_exception_details_withheld'
            raise Stop(row['outcome']) from None
        if type(result) is not dict:
            row['outcome'] = 'malformed_SDK_response'
            raise Stop('malformed_SDK_response')
        return result


def http_success(response, reader, name):
    metadata = response.get('ResponseMetadata')
    status = metadata.get('HTTPStatusCode') if type(metadata) is dict else None
    row = reader.evidence['read_outcomes'][name]
    row['http_status'] = status if type(status) is int and 100 <= status <= 599 else None
    require(type(status) is int and status == 200, 'response_HTTP_not_successful')
    row['outcome'] = 'HTTP200'


def hex_hash(value, length):
    return type(value) is str and bool(re.fullmatch('[a-f0-9]{' + str(length) + '}', value))


def code_hash(value):
    if type(value) is not str:
        return False
    try:
        raw = base64.b64decode(value, validate=True)
        return len(raw) == 32 and base64.b64encode(raw).decode() == value
    except Deadline:
        raise
    except Exception:
        return False


def positive_size(value, bound):
    return type(value) is int and 0 < value <= bound


def strict_json(raw):
    def pairs(rows):
        result = {}
        for key, value in rows:
            require(key not in result, 'receipt_JSON_invalid')
            result[key] = value
        return result

    def constant(_):
        raise Stop('receipt_JSON_invalid')
    try:
        return json.loads(raw, object_pairs_hook=pairs, parse_constant=constant)
    except Stop:
        raise
    except Exception:
        raise Stop('receipt_JSON_invalid') from None


def inspect(lam, s3, reader):
    evidence = reader.evidence
    response = reader.read(lam.get_function, FunctionName=FUNCTION)
    http_success(response, reader, 'get_function')
    cfg = response.get('Configuration')
    require(type(cfg) is dict and cfg.get('FunctionName') == FUNCTION
        and cfg.get('FunctionArn') == 'arn:aws:lambda:us-east-1:857687956942:function:' + FUNCTION
        and cfg.get('State') == 'Active' and cfg.get('LastUpdateStatus') == 'Successful'
        and code_hash(cfg.get('CodeSha256')) and positive_size(cfg.get('CodeSize'), 64 * BYTE_BOUND),
        'function_metadata_unqualified')
    evidence['actual_function'] = {'function': FUNCTION, 'code_sha256': cfg['CodeSha256'],
                                   'zip_bytes': cfg['CodeSize']}
    item = reader.read(s3.get_object, Bucket=BUCKET, Key=KEY)
    if item is MISSING:
        evidence['receipt_present'] = False
        return evidence
    body = item.get('Body')
    require(callable(getattr(body, 'read', None)) and callable(getattr(body, 'close', None)),
            'receipt_stream_missing')
    try:
        http_success(item, reader, 'get_object')
        try:
            raw = body.read(BYTE_BOUND + 1)
        except Deadline:
            raise
        except Exception:
            raise Stop('receipt_stream_read_failed') from None
        require(type(raw) is bytes and len(raw) <= BYTE_BOUND, 'receipt_byte_bound_or_type')
    finally:
        try:
            body.close()
        except Deadline:
            raise
        except Exception:
            raise Stop('receipt_stream_close_failed') from None
    receipt = strict_json(raw)
    require(type(receipt) is dict, 'receipt_shape_unqualified')
    member = receipt.get('source', {}).get('lambda_function.py') if type(receipt.get('source')) is dict else None
    evidence['receipt_present'] = True
    values = {key: receipt.get(key) for key in
        ('schema', 'function', 'commit', 'run_id', 'code_sha256', 'zip_sha256_hex', 'zip_bytes', 'verified')}
    values.update(handler_sha256=member.get('sha256') if type(member) is dict else None,
                  handler_bytes=member.get('bytes') if type(member) is dict else None)
    valid = {'schema': values['schema'] == 'release-receipt.v1', 'function': values['function'] == FUNCTION,
        'commit': hex_hash(values['commit'], 40),
        'run_id': type(values['run_id']) is str and bool(re.fullmatch('[0-9]{1,24}', values['run_id'])),
        'code_sha256': code_hash(values['code_sha256']), 'zip_sha256_hex': hex_hash(values['zip_sha256_hex'], 64),
        'zip_bytes': positive_size(values['zip_bytes'], 64 * BYTE_BOUND),
        'verified': type(values['verified']) is bool,
        'handler_sha256': hex_hash(values['handler_sha256'], 64),
        'handler_bytes': positive_size(values['handler_bytes'], BYTE_BOUND)}
    evidence['actual_receipt'] = {key: value if valid[key] else None for key, value in values.items()}
    evidence['receipt_field_validity'] = valid
    require(all(valid.values()) and values['verified'] is True, 'receipt_metadata_unqualified')
    evidence['comparisons'] = {
        'original_6470_commit_matches': receipt['commit'] == ORIGINAL_EXPECTED_COMMIT,
        'retained_release_commit_and_run_match': receipt['commit'] == RETAINED_RELEASE_COMMIT
            and receipt['run_id'] == RETAINED_RUN,
        'receipt_matches_current_function_hash_and_size': receipt['code_sha256'] == cfg['CodeSha256']
            and receipt['zip_bytes'] == cfg['CodeSize'],
        'receipt_hashes_self_consistent': base64.b64decode(receipt['code_sha256']).hex() == receipt['zip_sha256_hex'],
        'retained_package_and_handler_metadata_match': receipt['code_sha256'] == RETAINED_CODE_SHA
            and receipt['zip_sha256_hex'] == RETAINED_ZIP_SHA and receipt['zip_bytes'] == 1077860
            and member['sha256'] == RETAINED_HANDLER_SHA and member['bytes'] == 38171}
    return evidence


def safe_stop_reason(exc):
    known = {'read_scope_not_allowed', 'read_count_bound', 'access_denied',
        'client_error_details_withheld', 'transport_error_details_withheld',
        'unexpected_exception_details_withheld', 'malformed_SDK_response',
        'response_HTTP_not_successful', 'function_metadata_unqualified',
        'receipt_stream_missing', 'receipt_stream_read_failed', 'receipt_stream_close_failed',
        'receipt_byte_bound_or_type', 'receipt_JSON_invalid', 'receipt_shape_unqualified',
        'receipt_metadata_unqualified', 'diagnostic_time_bound'}
    if type(exc) in (Stop, Deadline) and len(exc.args) == 1 and type(exc.args[0]) is str and exc.args[0] in known:
        return exc.args[0]
    return 'unexpected_diagnostic_failure_details_withheld'


def main():
    require(os.environ.get('GITHUB_ACTIONS') == 'true', 'runner_only')
    import boto3
    from botocore.config import Config
    from botocore.exceptions import (ClientError, EndpointConnectionError,
        ConnectionClosedError, ConnectTimeoutError, ReadTimeoutError)
    sys.path.insert(0, str(ROOT / 'aws/ops'))
    from ops_report import report
    evidence = initial_evidence()
    reader = Reader(evidence, ClientError, (EndpointConnectionError, ConnectionClosedError,
                                          ConnectTimeoutError, ReadTimeoutError))

    def deadline(*_):
        raise Deadline()
    signal.signal(signal.SIGALRM, deadline)
    signal.alarm(45)
    with report(Path(__file__).stem) as out:
        try:
            cfg = Config(connect_timeout=3, read_timeout=8, retries={'total_max_attempts': 1})
            inspect(boto3.client('lambda', region_name='us-east-1', config=cfg),
                    boto3.client('s3', region_name='us-east-1', config=cfg), reader)
            out.kv(diagnostic_completed=True, aws_read_calls=reader.calls,
                   baseline_qualified=False, release_qualified=False)
        except Stop as exc:
            out.kv(diagnostic_completed=False, aws_read_calls=reader.calls, stop_reason=safe_stop_reason(exc))
            raise SystemExit(1) from None
        except Exception:
            out.kv(diagnostic_completed=False, aws_read_calls=reader.calls,
                   stop_reason='unexpected_diagnostic_failure_details_withheld')
            raise SystemExit(1) from None
        finally:
            signal.alarm(0)
            out.log('RECEIPT_PROVENANCE_JSON ' + json.dumps(evidence, sort_keys=True, allow_nan=False))


if __name__ == '__main__':
    main()
