"""Strict PR81 technical acceptance; literal release bindings required first.

17 allowlisted SDK reads and one bounded signed ZIP GET. No IAM/STS/LIST,
retained series bodies, writes, invokes, settings or baseline mutations.
Natural output acceptance and PR78 permission qualification are separate.
"""
import base64
from datetime import datetime, timezone
import hashlib
import importlib.util
import io
import json
import os
from pathlib import Path
import re
import signal
import subprocess
import sys
import urllib.request
import zipfile

ROOT = Path(__file__).resolve().parents[3]
HELPER = ROOT / 'aws/ops/staged/ops_6472_series_reliability_receipt_bound_capture.py'
HELPER_SHA256 = '5c6633b60d088139ef5b1382d1ed8f34f8703e9d230bb991ffe395fe186c3961'
if hashlib.sha256(HELPER.read_bytes()).hexdigest() != HELPER_SHA256:
    raise RuntimeError('immutable_capture_helper_changed')
spec = importlib.util.spec_from_file_location('immutable_series_capture_helpers', HELPER)
h = importlib.util.module_from_spec(spec)
spec.loader.exec_module(h)
Stop = h.Stop

BASELINE = ROOT / 'docs/ops/series-reliability-prospective-baseline.v1.json'
BASELINE_SHA256 = 'd1deae46d5ea268dfba5c34d558d5e0bb2bb04496c8f871152460ad75b9bfe7c'
PHASE = 'PR81'
SOURCE_SHA256 = 'd50f2c942d72a97af63cb7a581d10c4c8844a3aab7a3d7a64ccd5d319215f034'
# Bind ONLY independently verified actual normal merge/deployment evidence.
# Preparation is deliberately non-executable while these literals are unset.
EXPECTED_RELEASE_COMMIT = 'UNBOUND_RELEASE_COMMIT'
EXPECTED_RELEASE_RUN = 'UNBOUND_RELEASE_RUN'
MAX_CALLS = 17


class TimeBound(BaseException):
    def __init__(self):
        super().__init__('time_bound_reached')


def validate_bindings():
    h.require(PHASE == 'PR81' and SOURCE_SHA256 == h.RELEASE_SOURCES['PR81'],
              'release_phase_or_source_binding_invalid')
    h.require(type(EXPECTED_RELEASE_COMMIT) is str
              and re.fullmatch(r'[a-f0-9]{40}', EXPECTED_RELEASE_COMMIT)
              and type(EXPECTED_RELEASE_RUN) is str
              and re.fullmatch(r'[1-9][0-9]{0,19}', EXPECTED_RELEASE_RUN),
              'intended_release_binding_unset_or_invalid')


class Reader(h.Reader):
    def read(self, method, **kwargs):
        h.require(method.__name__ != 'simulate_principal_policy', 'postrelease_read_scope_not_allowed')
        h.require(self.calls < MAX_CALLS, 'api_bound_reached')
        # The immutable helper independently enforces the exact function,
        # receipt, five rule/target and one Scheduler request allowlist.
        result = super().read(method, **kwargs)
        # The frozen helper maps a typed receipt NoSuchKey to None. That
        # sentinel is fatal in inspect; it never qualifies a release.
        if result is None and method.__name__ == 'get_object':
            return None
        metadata = result.get('ResponseMetadata') if type(result) is dict else None
        valid = (type(metadata) is dict and type(metadata.get('HTTPStatusCode')) is int
                 and metadata['HTTPStatusCode'] == 200)
        if not valid and method.__name__ == 'get_object' and type(result) is dict:
            # A rejected streaming response must not leave its body open.
            try:
                result['Body'].close()
            except Exception:
                pass  # The response remains a fatal, redacted failure.
        h.require(valid, 'aws_read_response_unqualified')
        return result


def baseline():
    raw = BASELINE.read_bytes()
    h.require(hashlib.sha256(raw).hexdigest() == BASELINE_SHA256, 'prospective_baseline_bytes_changed')
    item = json.loads(raw)
    h.require(type(item) is dict
              and item.get('schema') == 'series-reliability-prospective-baseline.v1'
              and item.get('source_phase') == 'installed_PR75_prospective_predecessor'
              and item.get('approved_release_sources') == h.RELEASE_SOURCES
              and item.get('operating_fingerprint') == h.digest(item.get('operating_controls')),
              'prospective_baseline_invalid')
    return item


def inspect(lam, s3, events, scheduler, reader, opener=urllib.request.urlopen):
    validate_bindings()
    retained = baseline()
    source = ROOT / 'aws/lambdas' / h.FUNCTION / 'source/lambda_function.py'
    expected = source.read_bytes()
    h.require(hashlib.sha256(expected).hexdigest() == SOURCE_SHA256, 'checkout_handler_not_exact_PR81')
    h.require(h.commit_source(EXPECTED_RELEASE_COMMIT, source) == expected,
              'intended_commit_handler_not_exact_PR81')
    cfg = json.loads(source.parent.parent.joinpath('config.json').read_bytes())
    item = reader.read(lam.get_function, FunctionName=h.FUNCTION)
    live = item['Configuration']
    h.require(live.get('FunctionName') == h.FUNCTION
              and live.get('FunctionArn') == 'arn:aws:lambda:us-east-1:857687956942:function:' + h.FUNCTION
              and live.get('State') == 'Active' and live.get('LastUpdateStatus') == 'Successful',
              'function_not_ready')
    raw = h.bounded(opener(item['Code']['Location'], timeout=30), h.PACKAGE_BOUND)
    code_sha = base64.b64encode(hashlib.sha256(raw).digest()).decode()
    h.require(code_sha == live.get('CodeSha256') and type(live.get('CodeSize')) is int
              and live['CodeSize'] == len(raw), 'package_hash_mismatch')
    with zipfile.ZipFile(io.BytesIO(raw)) as archive:
        h.require(archive.namelist().count('lambda_function.py') == 1,
                  'source_member_not_unique')
        h.require(archive.getinfo('lambda_function.py').file_size <= h.SOURCE_BOUND,
                  'source_member_byte_bound')
        actual = archive.read('lambda_function.py')
    h.require(actual == expected, 'installed_handler_not_exact_PR81')
    runtime = h.runtime_evidence(live, reader.read(lam.get_runtime_management_config,
        FunctionName=h.FUNCTION, Qualifier='$LATEST'))
    controls = h.technical_controls(live, cfg)
    concurrency = reader.read(lam.get_function_concurrency,
                              FunctionName=h.FUNCTION).get('ReservedConcurrentExecutions')
    h.require(type(concurrency) is int and concurrency == 1, 'reserved_concurrency_not_one')
    controls.update(reserved_concurrency=concurrency, bindings={})
    for name in h.CLASSIC:
        rule = reader.read(events.describe_rule, Name=name)
        targets = reader.read(events.list_targets_by_rule, Rule=name, Limit=100)
        rows = targets.get('Targets')
        h.require(type(rows) is list and 0 < len(rows) <= 100 and not targets.get('NextToken'),
                  'target_row_bound_reached')
        h.require(rule.get('Name') == name and rule.get('State') == 'ENABLED'
                  and type(rule.get('ScheduleExpression')) is str and rule['ScheduleExpression'],
                  'named_rule_not_enabled')
        if name == h.CLASSIC[0]:
            h.require(any(row.get('Arn') == live['FunctionArn'] for row in rows), 'extractor_rule_unbound')
        controls['bindings'][name] = {
            'state': rule['State'], 'expression': rule['ScheduleExpression'],
            'rule_sha256': h.digest({key: value for key, value in rule.items() if key != 'ResponseMetadata'}),
            'targets_count': len(rows), 'targets_sha256': h.targets_projection(rows)}
    schedule = reader.read(scheduler.get_schedule, Name=h.SCHEDULER, GroupName='default')
    h.require(schedule.get('Name') == h.SCHEDULER and schedule.get('State') == 'ENABLED'
              and type(schedule.get('ScheduleExpression')) is str and schedule['ScheduleExpression']
              and type(schedule.get('Target')) is dict and schedule['Target'].get('Arn'),
              'protected_monitor_scheduler_not_enabled')
    controls['bindings'][h.SCHEDULER] = {
        'state': schedule['State'], 'expression': schedule['ScheduleExpression'],
        'schedule_sha256': h.digest({key: value for key, value in schedule.items()
            if key not in ('ResponseMetadata', 'CreationDate', 'LastModificationDate')})}
    receipt_item = reader.read(s3.get_object, Bucket=h.BUCKET, Key=h.RECEIPT_KEY)
    h.require(receipt_item is not None, 'release_receipt_missing')
    receipt = json.loads(h.bounded(receipt_item['Body'], h.SOURCE_BOUND))
    h.require(type(receipt) is dict and receipt.get('schema') == 'release-receipt.v1'
              and receipt.get('verified') is True and receipt.get('function') == h.FUNCTION
              and receipt.get('commit') == EXPECTED_RELEASE_COMMIT
              and type(receipt.get('run_id')) is str and receipt['run_id'] == EXPECTED_RELEASE_RUN
              and receipt.get('code_sha256') == code_sha
              and type(receipt.get('zip_bytes')) is int and receipt['zip_bytes'] == len(raw)
              and receipt.get('zip_sha256_hex') == hashlib.sha256(raw).hexdigest(),
              'intended_release_receipt_not_exact')
    deployed_at = h.receipt_clock(receipt.get('deployed_at'))
    members = receipt.get('source')
    member = members.get('lambda_function.py') if type(members) is dict else None
    h.require(type(member) is dict and member.get('sha256') == SOURCE_SHA256
              and type(member.get('bytes')) is int and member['bytes'] == len(actual),
              'receipt_handler_mismatch')
    sources = h.package_source_evidence(raw, EXPECTED_RELEASE_COMMIT, source)
    after = reader.read(lam.get_function, FunctionName=h.FUNCTION)['Configuration']
    after_runtime = h.runtime_evidence(after, reader.read(lam.get_runtime_management_config,
        FunctionName=h.FUNCTION, Qualifier='$LATEST'))
    h.require(after.get('FunctionName') == h.FUNCTION and after.get('FunctionArn') == live['FunctionArn']
              and after.get('State') == 'Active' and after.get('LastUpdateStatus') == 'Successful'
              and type(after.get('CodeSize')) is int and after['CodeSize'] == len(raw)
              and after.get('CodeSha256') == code_sha
              and h.technical_controls(after, cfg) == h.technical_controls(live, cfg)
              and after_runtime == runtime, 'observations_changed')
    controls['runtime_evidence'] = runtime
    h.require(controls == retained['operating_controls']
              and h.digest(controls) == retained['operating_fingerprint'],
              'prospective_controls_changed_acceptance_held')
    return {'purpose': 'strict PR81 postrelease technical acceptance', 'source_phase': PHASE,
        'intended_release_commit': EXPECTED_RELEASE_COMMIT, 'intended_release_run': EXPECTED_RELEASE_RUN,
        'handler_sha256': SOURCE_SHA256, 'code_sha256': code_sha,
        'zip_sha256_hex': hashlib.sha256(raw).hexdigest(), 'zip_bytes': len(raw),
        'deployed_at': deployed_at, 'package_sources': sources,
        'baseline_sha256': BASELINE_SHA256, 'baseline_compared': True,
        'operating_fingerprint': h.digest(controls), 'operating_controls': controls,
        'technical_acceptance_qualified': True, 'release_qualified': False,
        'natural_output_qualified': False, 'PR78_permission_qualified': False,
        'aws_read_calls': reader.calls, 'signed_package_gets': 1,
        'aws_writes': 0, 'producer_invokes': 0,
        'observed_at': datetime.now(timezone.utc).isoformat(),
        'probe_event_commit': subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=ROOT,
                                                       timeout=15).decode().strip(),
        'scope': 'Explicitly pinned PR81 release, whole ZIP/repository Python sources, receipt '
                 'and exact prospective controls including full runtime evidence. Function/runtime '
                 'twice; concurrency/receipt/six bindings once; no atomic cross-resource snapshot '
                 'or ABA/external/later-writer guarantee. Natural outputs and PR78 permission '
                 'remain separate. Historical failures unchanged; no rebaseline or repair.'}


def main():
    h.require(os.environ.get('GITHUB_ACTIONS') == 'true', 'runner_only')
    import boto3
    from botocore.config import Config
    from botocore.exceptions import (ClientError, EndpointConnectionError,
        ConnectionClosedError, ConnectTimeoutError, ReadTimeoutError)
    sys.path.insert(0, str(ROOT / 'aws/ops'))
    from ops_report import report
    reader = Reader(ClientError, (EndpointConnectionError, ConnectionClosedError,
                                 ConnectTimeoutError, ReadTimeoutError))

    def deadline(*_):
        raise TimeBound()

    signal.signal(signal.SIGALRM, deadline)
    signal.alarm(120)
    with report(Path(__file__).stem) as out:
        try:
            validate_bindings()  # No SDK client construction while unset.
            cfg = Config(connect_timeout=3, read_timeout=8, retries={'total_max_attempts': 1})
            clients = [boto3.client(service, region_name='us-east-1', config=cfg)
                       for service in ('lambda', 's3', 'events', 'scheduler')]
            result = inspect(*clients, reader)
            out.log('POSTRELEASE_JSON ' + json.dumps(result, sort_keys=True))
            out.kv(completed=True, technical_acceptance_qualified=True, release_qualified=False,
                   aws_read_calls=reader.calls, aws_writes=0, producer_invokes=0)
        except TimeBound:
            out.kv(completed=False, aws_read_calls=reader.calls, stop_reason='time_bound_reached')
            raise SystemExit(1) from None
        except Stop as exc:
            # Internal helper reasons are fixed/safe. Unexpected Stop text is
            # withheld, including injected SDK/parser/programming exceptions.
            reason = str(exc)
            safe_reasons = {'release_phase_or_source_binding_invalid',
                'intended_release_binding_unset_or_invalid', 'postrelease_read_scope_not_allowed',
                'api_bound_reached', 'read_scope_not_allowed', 'access_denied_stop',
                'aws_read_response_unqualified',
                'aws_read_failed_details_withheld', 'unexpected_aws_reader_failure_details_withheld',
                'prospective_baseline_bytes_changed', 'prospective_baseline_invalid',
                'checkout_handler_not_exact_PR81', 'intended_commit_handler_not_exact_PR81',
                'function_not_ready', 'byte_bound_reached', 'package_hash_mismatch',
                'source_member_not_unique', 'source_member_byte_bound', 'installed_handler_not_exact_PR81',
                'runtime_version_not_qualifiable', 'runtime_management_not_qualifiable',
                'runtime_manual_arn_mismatch', 'runtime_management_arn_unexpected',
                'runtime_management_response_not_successful', 'live_ephemeral_invalid',
                'live_environment_unavailable', 'reserved_concurrency_not_one', 'target_row_bound_reached',
                'named_rule_not_enabled', 'extractor_rule_unbound', 'protected_monitor_scheduler_not_enabled',
                'release_receipt_missing', 'intended_release_receipt_not_exact', 'receipt_clock_invalid',
                'receipt_handler_mismatch', 'package_source_inventory_unavailable',
                'package_source_inventory_unqualified', 'package_member_count_bound',
                'package_source_member_unqualified', 'package_repository_source_mismatch',
                'package_extra_python_source', 'observations_changed',
                'prospective_controls_changed_acceptance_held'}
            if type(exc) is h.RuntimeUnqualified:
                out.log('RUNTIME_UNQUALIFIED_JSON ' + json.dumps(exc.diagnostic, sort_keys=True))
            out.kv(completed=False, aws_read_calls=reader.calls,
                   stop_reason=reason if reason in safe_reasons else 'stop_details_withheld')
            raise SystemExit(1) from None
        except Exception:
            out.kv(completed=False, aws_read_calls=reader.calls,
                   stop_reason='unexpected_failure_details_withheld')
            raise SystemExit(1) from None
        finally:
            signal.alarm(0)


if __name__ == '__main__':
    main()
