"""Bounded Actions-only package and operating-control acceptance.

Reads one exact Lambda, its signed package (at most 64 MiB), six named schedule
bindings and its current release receipt. No checkpoint, archive, provider,
manifest body, log, metric or billing reads; no AWS writes or producer invokes.
Raw exceptions, signed URLs, environment values and target payloads are withheld.
The predecessor may lack a receipt only on the exact S3 NoSuchKey response.
A candidate requires the exact live ZIP, receipt-pinned committed handler and a
reviewed predecessor operating baseline. Handler bytes must match the checkout;
the receipt commit is independently bound to the exact released commit.
This technical check does not exercise checkpoint failure or runtime publication.
"""
import base64
from datetime import datetime
import hashlib
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
FUNCTION = 'justhodl-series-extractor'
BUCKET = 'justhodl-dashboard-live'
RECEIPT_KEY = 'data/ops/releases/' + FUNCTION + '.json'
PREDECESSOR_COMMIT = 'bfd28d6149e372d9f8d316be6d5cd56bcde2afbd'
PREDECESSOR_SHA256 = '9c82d0046498e7de75d5f24a8346047e0a59635972d438e65d1613331250a312'
BASELINE = ROOT / 'docs/ops/series-admission-baseline.json'
CLASSIC = ('justhodl-series-extractor-5min', 'cost-anomaly-daily',
           'fleet-error-monitor-5min', 'justhodl-d1-scan-daily',
           'justhodl-fleet-integrity-weekly')
SCHEDULER = 'fleet-error-monitor-sched'
MAX_CALLS = 14
PACKAGE_BOUND = 64 * 1024 * 1024
SOURCE_BOUND = 1024 * 1024
DEFAULT_DLQ = 'arn:aws:sqs:us-east-1:857687956942:justhodl-dlq-default'


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
    require(isinstance(raw, bytes) and len(raw) <= limit, 'byte_bound_reached')
    return raw


class Reader:
    def __init__(self):
        self.calls = 0

    def read(self, method, **kwargs):
        # An explicit read allowlist prevents accidental scope expansion.
        name = method.__name__
        allowed = {
            'get_function': {'FunctionName': FUNCTION},
            'get_function_concurrency': {'FunctionName': FUNCTION},
            'get_object': {'Bucket': BUCKET, 'Key': RECEIPT_KEY},
            'get_schedule': {'Name': SCHEDULER, 'GroupName': 'default'},
        }
        if name == 'describe_rule' and kwargs.get('Name') in CLASSIC:
            approved = {'Name': kwargs['Name']}
        elif name == 'list_targets_by_rule' and kwargs.get('Rule') in CLASSIC:
            approved = {'Rule': kwargs['Rule'], 'Limit': 100}
        else:
            approved = allowed.get(name)
        require(approved is not None and kwargs == approved, 'read_scope_not_allowed')
        require(self.calls < MAX_CALLS, 'api_bound_reached')
        self.calls += 1
        try:
            return method(**kwargs)
        except Exception as exc:
            response = getattr(exc, 'response', None)
            error = response.get('Error') if isinstance(response, dict) else None
            code = error.get('Code') if isinstance(error, dict) else None
            if code == 'NoSuchKey' and name == 'get_object':
                return None
            raise Stop('access_denied_stop' if code in
                       ('AccessDenied', 'AccessDeniedException', 'UnauthorizedOperation', '403')
                       else 'aws_read_failed_details_withheld') from None


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, default=str,
                                    allow_nan=False, separators=(',', ':')).encode()).hexdigest()


def hidden(value):
    # A whole-field digest preserves equality checks without publishing private values.
    return {'sha256': digest(value)}


def targets_projection(target):
    # Include every target field, including payload/role/retry/service-specific fields,
    # in the private digest so a payload change cannot pass baseline comparison.
    return digest(sorted(target, key=lambda row: str(row.get('Id'))))


def commit_source(commit, source):
    require(isinstance(commit, str) and re.fullmatch('[a-fA-F0-9]{40}', commit), 'receipt_commit_invalid')
    try:
        raw = subprocess.check_output(['git', 'show', commit + ':' + str(source.relative_to(ROOT))],
                                      cwd=ROOT, stderr=subprocess.PIPE, timeout=15)
    except Exception:
        raise Stop('commit_source_unavailable') from None
    require(len(raw) <= SOURCE_BOUND, 'source_member_byte_bound')
    return raw


def receipt_clock(value):
    require(isinstance(value, str) and re.fullmatch(
        r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,6})?(?:Z|[+-]\d{2}:\d{2})', value),
        'receipt_clock_invalid')
    try:
        parsed = datetime.fromisoformat(value.replace('Z', '+00:00'))
        require(parsed.tzinfo is not None, 'receipt_clock_invalid')
        return parsed.isoformat()
    except ValueError:
        raise Stop('receipt_clock_invalid') from None


def technical_controls(live, cfg):
    ephemeral = (live.get('EphemeralStorage') or {}).get('Size')
    require(type(ephemeral) is int and 512 <= ephemeral <= 10240, 'live_ephemeral_invalid')
    environment = live.get('Environment') or {}
    variables = environment.get('Variables') or {}
    require(isinstance(variables, dict) and not environment.get('Error'), 'live_environment_unavailable')
    controls = {
        'runtime_matches': live.get('Runtime') == cfg['runtime'],
        'handler_matches': live.get('Handler') == cfg['handler'],
        'memory_matches': live.get('MemorySize') == cfg['memory'],
        'timeout_matches': live.get('Timeout') == cfg['timeout'],
        'description_matches': live.get('Description') == cfg['description'],
        'role_matches': live.get('Role') == cfg['role'],
        'declared_environment_matches': all(variables.get(k) == v for k, v in cfg['env'].items()),
        # Only canonical ephemeral_storage is applied by the existing deploy lane.
        'ephemeral_matches_managed_configuration': 'ephemeral_storage' not in cfg
            or ephemeral == cfg['ephemeral_storage'],
        'tracing_already_active': (live.get('TracingConfig') or {}).get('Mode') == 'Active',
        'dlq_already_standard': (live.get('DeadLetterConfig') or {}).get('TargetArn') == DEFAULT_DLQ,
    }
    require(all(controls.values()), 'release_control_mismatch:' + ','.join(k for k, v in controls.items() if not v))
    # Omit code hashes, revisions and deployment clocks, which necessarily change
    # on release. Preserve stable technical controls, with private fields digested.
    public_fields = ('Runtime', 'Handler', 'MemorySize', 'Timeout', 'Architectures',
                     'EphemeralStorage', 'PackageType', 'TracingConfig')
    private_fields = ('Description', 'Role', 'Environment', 'DeadLetterConfig',
                      'VpcConfig', 'Layers', 'FileSystemConfigs', 'KMSKeyArn',
                      'LoggingConfig', 'SnapStart', 'RuntimeVersionConfig', 'MasterArn')
    return {'configuration_matches': controls,
            'technical_configuration': {k: live.get(k) for k in public_fields},
            'private_configuration': {k: hidden(live.get(k)) for k in private_fields}}


def inspect(lam, s3, events, scheduler, reader, opener=urllib.request.urlopen):
    source = ROOT / 'aws/lambdas' / FUNCTION / 'source/lambda_function.py'
    expected = source.read_bytes()
    before = commit_source(PREDECESSOR_COMMIT, source)
    require(hashlib.sha256(before).hexdigest() == PREDECESSOR_SHA256, 'predecessor_source_pin_mismatch')
    cfg = json.loads(source.parent.parent.joinpath('config.json').read_bytes())
    item = reader.read(lam.get_function, FunctionName=FUNCTION)
    live = item['Configuration']
    require(live.get('FunctionName') == FUNCTION
            and live.get('FunctionArn') == 'arn:aws:lambda:us-east-1:857687956942:function:' + FUNCTION
            and live.get('State') == 'Active'
            and live.get('LastUpdateStatus') == 'Successful', 'function_not_ready')
    raw = bounded(opener(item['Code']['Location'], timeout=30), PACKAGE_BOUND)
    code_sha256 = base64.b64encode(hashlib.sha256(raw).digest()).decode()
    require(code_sha256 == live.get('CodeSha256') and live.get('CodeSize') == len(raw), 'package_hash_mismatch')
    with zipfile.ZipFile(io.BytesIO(raw)) as archive:
        require(archive.namelist().count('lambda_function.py') == 1, 'source_member_not_unique')
        require(archive.getinfo('lambda_function.py').file_size <= SOURCE_BOUND, 'source_member_byte_bound')
        actual = archive.read('lambda_function.py')
    require(actual in (before, expected), 'live_handler_not_reviewed')
    handler_sha256 = hashlib.sha256(actual).hexdigest()
    phase = 'predecessor' if actual == before else 'candidate'
    require(phase == 'predecessor' or BASELINE.exists(), 'candidate_baseline_missing')
    operating = technical_controls(live, cfg)
    concurrency = reader.read(lam.get_function_concurrency, FunctionName=FUNCTION).get('ReservedConcurrentExecutions')
    require(type(concurrency) is int and concurrency == 1, 'reserved_concurrency_not_one')
    operating.update(reserved_concurrency=concurrency, bindings={})
    for name in CLASSIC:
        rule = reader.read(events.describe_rule, Name=name)
        targets = reader.read(events.list_targets_by_rule, Rule=name, Limit=100)
        rows = targets.get('Targets')
        require(isinstance(rows, list) and 0 < len(rows) <= 100 and not targets.get('NextToken'), 'target_row_bound_reached')
        require(rule.get('Name') == name and rule.get('State') == 'ENABLED'
                and isinstance(rule.get('ScheduleExpression'), str) and rule['ScheduleExpression'], 'named_rule_not_enabled')
        if name == CLASSIC[0]:
            require(any(t.get('Arn') == live.get('FunctionArn') for t in rows), 'extractor_rule_unbound')
        operating['bindings'][name] = {
            'state': rule['State'], 'expression': rule['ScheduleExpression'],
            'rule_sha256': digest({k: v for k, v in rule.items() if k != 'ResponseMetadata'}),
            'targets_count': len(rows), 'targets_sha256': targets_projection(rows)}
    schedule = reader.read(scheduler.get_schedule, Name=SCHEDULER, GroupName='default')
    require(schedule.get('Name') == SCHEDULER and schedule.get('State') == 'ENABLED'
            and isinstance(schedule.get('ScheduleExpression'), str) and schedule['ScheduleExpression']
            and isinstance(schedule.get('Target'), dict) and schedule['Target'].get('Arn'), 'protected_monitor_scheduler_not_enabled')
    # Creation/last-modification clocks are evidence, not operating settings.
    operating['bindings'][SCHEDULER] = {
        'state': schedule['State'], 'expression': schedule['ScheduleExpression'],
        'schedule_sha256': digest({k: v for k, v in schedule.items()
                                 if k not in ('ResponseMetadata', 'CreationDate', 'LastModificationDate')})}
    fingerprint = digest(operating)
    baseline_exists = BASELINE.exists()
    if baseline_exists:
        baseline = json.loads(BASELINE.read_bytes())
        require(baseline.get('source_phase') == 'predecessor'
                and baseline.get('handler_sha256') == PREDECESSOR_SHA256
                and baseline.get('operating_fingerprint') == digest(baseline.get('operating_controls')),
                'baseline_invalid')
        require(fingerprint == baseline['operating_fingerprint'], 'operating_controls_changed')
    receipt_item = reader.read(s3.get_object, Bucket=BUCKET, Key=RECEIPT_KEY)
    receipt = json.loads(bounded(receipt_item['Body'], SOURCE_BOUND)) if receipt_item is not None else None
    receipt_commit, deployed_at = None, None
    if receipt is not None:
        require(isinstance(receipt, dict), 'receipt_shape_invalid')
        receipt_commit = receipt.get('commit')
        require(isinstance(receipt_commit, str) and re.fullmatch('[a-fA-F0-9]{40}', receipt_commit), 'receipt_commit_invalid')
        deployed_at = receipt_clock(receipt.get('deployed_at'))
        require(receipt.get('schema') == 'release-receipt.v1' and receipt.get('verified') is True
                and receipt.get('function') == FUNCTION and receipt.get('code_sha256') == code_sha256
                and type(receipt.get('zip_bytes')) is int and receipt['zip_bytes'] == len(raw)
                and receipt.get('zip_sha256_hex') == hashlib.sha256(raw).hexdigest()
                and isinstance(receipt.get('source'), dict), 'release_receipt_mismatch')
        member = receipt['source'].get('lambda_function.py')
        require(isinstance(member, dict) and member.get('sha256') == handler_sha256
                and type(member.get('bytes')) is int and member['bytes'] == len(actual), 'receipt_handler_mismatch')
        require(commit_source(receipt_commit, source) == actual, 'receipt_commit_source_mismatch')
    if phase == 'candidate':
        require(baseline_exists, 'candidate_baseline_missing')
        require(receipt is not None, 'candidate_receipt_missing')
    return {'source_phase': phase, 'handler_sha256': handler_sha256,
            'code_sha256': code_sha256, 'zip_sha256_hex': hashlib.sha256(raw).hexdigest(),
            'zip_bytes': len(raw), 'receipt_commit': receipt_commit, 'deployed_at': deployed_at,
            'operating_fingerprint': fingerprint, 'operating_controls': operating,
            'baseline_compared': baseline_exists, 'named_bindings_checked': len(operating['bindings']),
            'aws_read_calls': reader.calls, 'signed_package_gets': 1, 'aws_writes': 0, 'producer_invokes': 0,
            'scope': 'Exact live ZIP and receipt-pinned handler, projected technical controls and six named enabled schedule bindings. Other package source members, arbitrary bindings and runtime failure/publication paths are not independently qualified. Private controls and complete targets are compared as digests; no private bodies are published.'}


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
