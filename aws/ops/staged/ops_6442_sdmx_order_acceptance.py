"""Bounded runner-only package/control check; no producer invocation or AWS writes.

Reads one Lambda package, technical configuration, ten named schedule bindings,
the release receipt and current summary metadata. Never reads retained data,
billing, metrics, logs or private application bodies. Raw errors, signed URLs,
environment and target payloads are withheld from the public report.
"""
import base64
from datetime import datetime, timezone
import hashlib
import io
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import urllib.request
import zipfile

ROOT = Path(__file__).resolve().parents[3]
FUNCTION = 'justhodl-sdmx-walker'
BUCKET = 'justhodl-dashboard-live'
BASELINE = ROOT / 'docs/ops/sdmx-order-baseline.json'
CLASSIC = ('justhodl-sdmx-walker-hourly', 'cost-anomaly-daily',
           'fleet-error-monitor-5min', 'justhodl-d1-scan-daily',
           'justhodl-fleet-integrity-weekly')
SCHEDULERS = ('justhodl-oecd-retry-hourly', 'justhodl-statcan-retry-hourly',
              'justhodl-eurostat-retry-30min', 'justhodl-ecb-rewalk-weekly',
              'fleet-error-monitor-sched')
MAX_CALLS = 20
PACKAGE_BOUND = 64 * 1024 * 1024


class Stop(Exception):
    pass


def require(condition, reason):
    if not condition:
        raise Stop(reason)


def bounded(stream, limit):
    try:
        raw = stream.read(limit + 1)
    finally:
        stream.close()
    require(len(raw) <= limit, 'byte_bound_reached')
    return raw


class Reader:
    def __init__(self):
        self.calls = 0
    def read(self, method, **kwargs):
        require(self.calls < MAX_CALLS, 'api_bound_reached')
        self.calls += 1
        try:
            return method(**kwargs)
        except Exception as exc:
            code = str(getattr(exc, 'response', {}).get('Error', {}).get('Code', ''))
            if code == 'NoSuchKey' and method.__name__ == 'get_object':
                return None
            raise Stop('access_denied_stop' if code in ('AccessDenied', 'AccessDeniedException', 'UnauthorizedOperation', '403')
                       else 'aws_read_failed_details_withheld') from None


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, default=str).encode()).hexdigest()


def target_projection(target):
    # Target payload content and hashes are deliberately excluded.
    return {k: target.get(k) for k in ('Arn', 'Id', 'RetryPolicy', 'DeadLetterConfig')}


def inspect(lam, s3, events, scheduler, reader, opener=urllib.request.urlopen):
    source = ROOT / 'aws/lambdas' / FUNCTION / 'source/lambda_function.py'
    expected = source.read_bytes()
    before = subprocess.check_output(['git', 'show',
        'ab0a502c047c1399815c9c081fe633d9dba3d608:' + str(source.relative_to(ROOT))], cwd=ROOT)
    cfg = json.loads(source.parent.parent.joinpath('config.json').read_bytes())
    item = reader.read(lam.get_function, FunctionName=FUNCTION)
    live = item['Configuration']
    require(live.get('FunctionName') == FUNCTION and live.get('State') == 'Active'
            and live.get('LastUpdateStatus') == 'Successful', 'function_not_ready')
    raw = bounded(opener(item['Code']['Location'], timeout=30), PACKAGE_BOUND)
    require(base64.b64encode(hashlib.sha256(raw).digest()).decode() == live['CodeSha256'], 'package_hash_mismatch')
    with zipfile.ZipFile(io.BytesIO(raw)) as archive:
        require(archive.getinfo('lambda_function.py').file_size <= 1024 * 1024, 'source_member_byte_bound')
        actual = archive.read('lambda_function.py')
        require(actual in (before, expected), 'live_handler_not_reviewed')
    phase = 'predecessor' if actual == before else 'candidate'
    controls = {
        'runtime_matches': live.get('Runtime') == cfg['runtime'],
        'handler_matches': live.get('Handler') == cfg['handler'],
        'memory_matches': live.get('MemorySize') == cfg['memory'],
        'timeout_matches': live.get('Timeout') == cfg['timeout'],
        'ephemeral_matches': live.get('EphemeralStorage', {}).get('Size') == cfg['ephemeral_mb'],
        'role_matches': live.get('Role') == cfg['role'],
        'declared_environment_matches': not (live.get('Environment') or {}).get('Error')
            and all((live.get('Environment') or {}).get('Variables', {}).get(k) == v for k, v in cfg['env'].items()),
        'tracing_already_active': live.get('TracingConfig', {}).get('Mode') == 'Active',
        'dlq_already_standard': live.get('DeadLetterConfig', {}).get('TargetArn')
            == 'arn:aws:sqs:us-east-1:857687956942:justhodl-dlq-default',
    }
    require(all(controls.values()), 'release_control_mismatch')
    concurrency = reader.read(lam.get_function_concurrency, FunctionName=FUNCTION).get('ReservedConcurrentExecutions')
    operating = {'configuration_matches': controls, 'reserved_concurrency': concurrency,
                 'architectures': live.get('Architectures'), 'bindings': {}}
    for name in CLASSIC:
        rule = reader.read(events.describe_rule, Name=name)
        targets = reader.read(events.list_targets_by_rule, Rule=name, Limit=100)
        require(isinstance(targets.get('Targets'), list) and not targets.get('NextToken'), 'target_row_bound_reached')
        require(rule.get('Name') == name and rule.get('State') == 'ENABLED', 'named_rule_not_enabled')
        if name == CLASSIC[0]:
            require(any(t.get('Arn') == live.get('FunctionArn') for t in targets['Targets']), 'walker_rule_unbound')
        operating['bindings'][name] = {k: rule.get(k) for k in ('State', 'ScheduleExpression', 'EventBusName')}
        operating['bindings'][name]['targets'] = sorted([
            target_projection(t) | {'role_present': bool(t.get('RoleArn')),
            'payload_fields_present': [k for k in ('Input', 'InputPath', 'InputTransformer') if k in t]}
            for t in targets['Targets']], key=lambda t: str(t.get('Id')))
    for name in SCHEDULERS:
        schedule = reader.read(scheduler.get_schedule, Name=name, GroupName='default')
        require(schedule.get('Name') == name and schedule.get('State') == 'ENABLED', 'named_scheduler_not_enabled')
        target = schedule.get('Target') or {}
        if name != SCHEDULERS[-1]:
            require(target.get('Arn') == live.get('FunctionArn'), 'walker_scheduler_unbound')
        operating['bindings'][name] = {k: schedule.get(k) for k in ('State', 'ScheduleExpression',
            'ScheduleExpressionTimezone', 'FlexibleTimeWindow', 'StartDate', 'EndDate', 'ActionAfterCompletion')}
        operating['bindings'][name]['target'] = target_projection(target) | {
            'role_present': bool(target.get('RoleArn')), 'input_present': 'Input' in target}
    fingerprint = digest(operating)
    if BASELINE.exists():
        baseline = json.loads(BASELINE.read_bytes())
        require(fingerprint == baseline['operating_fingerprint'], 'operating_controls_changed')
    receipt_item = reader.read(s3.get_object, Bucket=BUCKET, Key='data/ops/releases/' + FUNCTION + '.json')
    receipt = json.loads(bounded(receipt_item['Body'], 1024 * 1024)) if receipt_item else None
    if phase == 'candidate':
        require(receipt and receipt.get('verified') is True and receipt.get('function') == FUNCTION
                and receipt.get('code_sha256') == live['CodeSha256']
                and receipt.get('source', {}).get('lambda_function.py', {}).get('sha256') == hashlib.sha256(expected).hexdigest(),
                'candidate_receipt_mismatch')
        commit_source = subprocess.check_output(['git', 'show', receipt['commit'] + ':' + str(source.relative_to(ROOT))], cwd=ROOT)
        require(commit_source == expected, 'receipt_commit_source_mismatch')
    summary = reader.read(s3.head_object, Bucket=BUCKET, Key='data/warm/sdmx-walker-summary.json')
    return {'source_phase': phase, 'handler_sha256': hashlib.sha256(actual).hexdigest(),
            'code_sha256': live['CodeSha256'], 'receipt_commit': receipt.get('commit') if receipt else None,
            'deployed_at': receipt.get('deployed_at') if receipt else None,
            'operating_fingerprint': fingerprint, 'operating_controls': operating,
            'baseline_compared': BASELINE.exists(), 'named_bindings_checked': len(operating['bindings']),
            'summary_last_modified': str(summary.get('LastModified')), 'summary_bytes': summary.get('ContentLength'),
            'aws_read_calls': reader.calls, 'signed_package_gets': 1, 'aws_writes': 0, 'producer_invokes': 0,
            'scope': 'Exact handler/package/receipt, technical controls and ten named enabled schedules. Other package members, undeclared environment values, payload contents, arbitrary bindings and AWS billing are not measured.'}


def main():
    require(os.environ.get('GITHUB_ACTIONS') == 'true', 'runner_only')
    import boto3
    from botocore.config import Config
    sys.path.insert(0, str(ROOT / 'aws/ops'))
    from ops_report import report
    reader = Reader()
    def deadline(*_):
        raise Stop('time_bound_reached')
    signal.signal(signal.SIGALRM, deadline)
    signal.alarm(120)
    with report(Path(__file__).stem) as out:
        try:
            cfg = Config(connect_timeout=3, read_timeout=8, retries={'total_max_attempts': 1})
            clients = [boto3.client(service, region_name='us-east-1', config=cfg)
                       for service in ('lambda', 's3', 'events', 'scheduler')]
            result = inspect(*clients, reader)
            out.log('TECHNICAL_JSON ' + json.dumps(result, sort_keys=True, default=str))
            out.kv(completed=True, source_phase=result['source_phase'], aws_read_calls=reader.calls,
                   aws_writes=0, producer_invokes=0)
        except Stop as exc:
            out.kv(completed=False, aws_read_calls=reader.calls, stop_reason=str(exc))
            raise SystemExit(1) from None
        except Exception:
            out.kv(completed=False, aws_read_calls=reader.calls, stop_reason='unexpected_failure_details_withheld')
            raise SystemExit(1) from None
        finally:
            signal.alarm(0)


if __name__ == '__main__':
    main()
