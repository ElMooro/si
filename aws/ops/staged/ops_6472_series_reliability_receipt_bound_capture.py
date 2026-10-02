"""Capture a NEW prospective PR78/81 baseline; historical PR75 remains failed.

Actions-only, exact installed package/receipt, six existing bindings, full valid
runtime ARN and separate runtime-management mode. No retained-data/archive bodies,
LIST requests, AWS writes, invokes or setting changes. Four bounded role policy
simulations are disclosed as simulations, never as live role LIST proof.
The original baseline/probe/report are neither replaced nor reinterpreted.
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
PREDECESSOR_COMMIT = 'cb8a371c361ec11f7220a303f062bd16123c0823'
PREDECESSOR_SHA256 = 'bcd80ce358aa433d9de33c45b9fb2900987c63046d343462b3c359b7c3724867'
BASELINE = ROOT / 'docs/ops/series-reliability-prospective-baseline.v1.json'
HISTORICAL_BASELINE = ROOT / 'docs/ops/series-admission-baseline.json'
HISTORICAL_SHA256 = '4f4937b34ce424e80adfde5c674ee88ae10652db6e96eb6e4a6f087f4dad0e09'
INSTALLED_RELEASE = 'e5a4c11cd44fc103ae64a0ca43a7161c31ec7808'
INSTALLED_RELEASE_RUN = '36982007891'
INSTALLED_CODE_SHA256 = 'w1YX8/Ni49DJkBRIUpoksNzABPkd/rF+d86eX0LtXtY='
ROLE = 'arn:aws:iam::857687956942:role/lambda-execution-role'
RELEASE_SOURCES = {
    'PR78': '3c0aeb7a3ea3454a9296c8d832044376c2283bac0b3885f21773c5ea7dfb2cfb',
    'PR81': 'd50f2c942d72a97af63cb7a581d10c4c8844a3aab7a3d7a64ccd5d319215f034',
    'combined': '2e5fd09a4dac47275812b8e8955916af6f913395a8a9918933d1dfe362cccf33'}
CLASSIC = ('justhodl-series-extractor-5min', 'cost-anomaly-daily',
           'fleet-error-monitor-5min', 'justhodl-d1-scan-daily',
           'justhodl-fleet-integrity-weekly')
SCHEDULER = 'fleet-error-monitor-sched'
MAX_CALLS = 21
PACKAGE_BOUND = 64 * 1024 * 1024
SOURCE_BOUND = 1024 * 1024
DEFAULT_DLQ = 'arn:aws:sqs:us-east-1:857687956942:justhodl-dlq-default'


class Stop(Exception):
    pass


class RuntimeUnqualified(Stop):
    def __init__(self, value):
        super().__init__('runtime_version_not_qualifiable')
        shape = runtime_version_shape(value)
        self.diagnostic = {'observed_runtime_version_shape': shape}
        if shape['runtime_arn_syntax_valid']:
            self.diagnostic['observed_runtime_version_arn'] = value['RuntimeVersionArn']
        # Unqualifying Error messages/unknown fields are withheld, not treated
        # as a qualifying digest. Full qualifying values are retained on success.


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
    def __init__(self, client_error_type=(), transport_error_types=()):
        self.calls = 0
        self.client_error_type = client_error_type
        self.transport_error_types = transport_error_types

    def read(self, method, **kwargs):
        # An explicit read allowlist prevents accidental scope expansion.
        name = method.__name__
        allowed = {
            'get_function': {'FunctionName': FUNCTION},
            'get_runtime_management_config': {'FunctionName': FUNCTION, 'Qualifier': '$LATEST'},
            'get_function_concurrency': {'FunctionName': FUNCTION},
            'get_object': {'Bucket': BUCKET, 'Key': RECEIPT_KEY},
            'get_schedule': {'Name': SCHEDULER, 'GroupName': 'default'},
        }
        if name == 'simulate_principal_policy':
            approved = next((row for row in simulation_requests() if row == kwargs), None)
        elif name == 'describe_rule' and kwargs.get('Name') in CLASSIC:
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
            if not isinstance(exc, self.client_error_type):
                if isinstance(exc, self.transport_error_types):
                    raise Stop('aws_read_failed_details_withheld') from None
                raise Stop('unexpected_aws_reader_failure_details_withheld') from None
            response = getattr(exc, 'response', None)
            error = response.get('Error') if isinstance(response, dict) else None
            code = error.get('Code') if isinstance(error, dict) else None
            if (isinstance(exc, self.client_error_type)
                    and code == 'NoSuchKey' and name == 'get_object'):
                return None
            raise Stop('access_denied_stop' if code in
                       ('AccessDenied', 'AccessDeniedException', 'UnauthorizedOperation', '403')
                       else 'aws_read_failed_details_withheld') from None


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, default=str,
                                    allow_nan=False, separators=(',', ':')).encode()).hexdigest()


def runtime_version_shape(value):
    # Only shape is reported, even when Lambda returns RuntimeVersionConfig.Error.
    def kind(item):
        return {dict: 'object', list: 'array', str: 'string', bool: 'boolean',
                int: 'number', float: 'number', type(None): 'null'}.get(type(item), 'other')

    fields = value if isinstance(value, dict) else {}
    arn = fields.get('RuntimeVersionArn')
    # AWS RuntimeVersionConfig documented ARN pattern/length, syntax only:
    # https://docs.aws.amazon.com/lambda/latest/api/API_RuntimeVersionConfig.html
    valid_arn = isinstance(arn, str) and 26 <= len(arn) <= 2048 and bool(re.fullmatch(
        r'arn:(aws[a-zA-Z-]*):lambda:[a-z]{2}((-gov)|(-iso(b?)))?-[a-z]+-\d{1}::runtime:.+', arn))
    return {'value_type': kind(value), 'error_field_present': 'Error' in fields,
            'error_is_object': isinstance(fields.get('Error'), dict),
            'runtime_arn_field_present': 'RuntimeVersionArn' in fields,
            'runtime_arn_type': kind(arn), 'runtime_arn_syntax_valid': valid_arn,
            'unrecognized_fields_present': any(k not in ('Error', 'RuntimeVersionArn') for k in fields)}


def control_differences(before, after):
    # Paths come only from the probe's known current projection. Compare each
    # control atomically: never descend into environment values or target bodies.
    rows = []

    def changed(path, old, new):
        old_digest, new_digest = digest(old), digest(new)
        if old_digest != new_digest:
            rows.append({'path': path, 'before_sha256': old_digest,
                         'after_sha256': new_digest})

    changed('reserved_concurrency', before.get('reserved_concurrency'), after.get('reserved_concurrency'))
    for group in ('configuration_matches', 'technical_configuration', 'private_configuration', 'bindings'):
        old, new = before.get(group), after.get(group)
        if not isinstance(old, dict) or not isinstance(new, dict):
            changed(group, old, new)
            continue
        for name in sorted(new):
            changed(group + '.' + name, old.get(name), new[name])
        # Unknown baseline field names are digested, never printed as paths.
        changed(group + '.field_inventory', sorted(old), sorted(new))
    return rows


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


def runtime_evidence(configuration, management):
    """Preserve qualifying nonsecret runtime values, never environment fields.

    Error/unknown fields cannot qualify this prospective baseline. Their shape
    is reported separately by the caller on refusal; raw errors are withheld.
    """
    value = configuration.get('RuntimeVersionConfig')
    if not (type(value) is dict and set(value) == {'RuntimeVersionArn'}
            and runtime_version_shape(value)['runtime_arn_syntax_valid']):
        raise RuntimeUnqualified(value)
    require(type(management) is dict
            and not set(management) - {'FunctionArn', 'UpdateRuntimeOn', 'RuntimeVersionArn', 'ResponseMetadata'}
            and management.get('FunctionArn') in (
                'arn:aws:lambda:us-east-1:857687956942:function:' + FUNCTION,
                'arn:aws:lambda:us-east-1:857687956942:function:' + FUNCTION + ':$LATEST')
            and management.get('UpdateRuntimeOn') in ('Auto', 'Manual', 'FunctionUpdate'),
            'runtime_management_not_qualifiable')
    mode = management['UpdateRuntimeOn']
    managed_arn = management.get('RuntimeVersionArn')
    if mode == 'Manual':
        require(managed_arn == value['RuntimeVersionArn'], 'runtime_manual_arn_mismatch')
    else:
        require(managed_arn is None, 'runtime_management_arn_unexpected')
    metadata = management.get('ResponseMetadata')
    require(type(metadata) is dict and type(metadata.get('HTTPStatusCode')) is int
            and metadata['HTTPStatusCode'] == 200,
            'runtime_management_response_not_successful')
    return {'runtime_version_config': value,
            'runtime_version_shape': runtime_version_shape(value),
            'runtime_management': {key: management[key] for key in
                ('FunctionArn', 'UpdateRuntimeOn', 'RuntimeVersionArn') if key in management},
            'runtime_management_arn_present': 'RuntimeVersionArn' in management}


def simulation_requests():
    # Existing code supports only eurostat/ecb. Never simulate arbitrary actions,
    # resources, principals, extra policies or context that grants permission.
    rows = []
    for provider in ('eurostat', 'ecb'):
        for tail in ('series/', 'series-manifest.json'):
            rows.append({'PolicySourceArn': ROLE, 'ActionNames': ['s3:ListBucket'],
                'ResourceArns': ['arn:aws:s3:::' + BUCKET], 'MaxItems': 1,
                'ContextEntries': [
                    {'ContextKeyName': 's3:prefix', 'ContextKeyValues': [
                        'data/providers/' + provider + '/' + tail], 'ContextKeyType': 'string'},
                    {'ContextKeyName': 's3:delimiter', 'ContextKeyValues': ['/'], 'ContextKeyType': 'string'},
                    {'ContextKeyName': 's3:max-keys', 'ContextKeyValues': ['1'], 'ContextKeyType': 'numeric'}]})
    return rows


def simulation_evidence(iam, reader):
    decisions = []
    for request in simulation_requests():
        result = reader.read(iam.simulate_principal_policy, **request)
        metadata = result.get('ResponseMetadata')
        require(type(metadata) is dict and type(metadata.get('HTTPStatusCode')) is int
                and metadata['HTTPStatusCode'] == 200, 'list_simulation_response_unqualified')
        rows = result.get('EvaluationResults')
        require(type(rows) is list and len(rows) == 1
                and result.get('IsTruncated') is False and not result.get('Marker'),
                'list_simulation_response_unqualified')
        row = rows[0]
        require(type(row) is dict and row.get('EvalActionName') == 's3:ListBucket'
                and row.get('EvalResourceName') == 'arn:aws:s3:::' + BUCKET
                and row.get('EvalDecision') in ('allowed', 'implicitDeny', 'explicitDeny'),
                'list_simulation_response_unqualified')
        for detail, key in (('PermissionsBoundaryDecisionDetail', 'AllowedByPermissionsBoundary'),
                            ('OrganizationsDecisionDetail', 'AllowedByOrganizations')):
            value = row.get(detail, {})
            require(type(value) is dict and not set(value) - {key}
                    and (key not in value or type(value[key]) is bool),
                    'list_simulation_response_unqualified')
        require(type(row.get('MissingContextValues', [])) is list,
                'list_simulation_response_unqualified')
        decisions.append({'scope_index': len(decisions), 'decision': row['EvalDecision'],
            'missing_context': bool(row.get('MissingContextValues')),
            'boundary_allows': row.get('PermissionsBoundaryDecisionDetail', {}).get('AllowedByPermissionsBoundary'),
            'organizations_allows': row.get('OrganizationsDecisionDetail', {}).get('AllowedByOrganizations')})
    return {'requests': simulation_requests(), 'decisions': decisions,
            'all_identity_simulations_allowed': all(row['decision'] == 'allowed' and not row['missing_context']
                and row['boundary_allows'] is not False and row['organizations_allows'] is not False for row in decisions),
            'native_execution_role_LIST_proven': False,
            'scope': 'IAM role policy simulation only; resource-policy/live-request results are not proven. No LIST or object-body read is performed.'}


def package_source_evidence(raw, receipt_commit, source):
    # Verify every shipped repository Python source against the receipt commit,
    # including the shared bundle. The signed ZIP/CodeSha binds all package bytes;
    # bytecode/build metadata are not independently reconstructed here.
    try:
        names = subprocess.check_output(['git', 'ls-tree', '-r', '--name-only', receipt_commit,
            '--', 'aws/shared', str(source.parent.relative_to(ROOT))], cwd=ROOT,
            stderr=subprocess.PIPE, timeout=15).decode().splitlines()
    except Exception:
        raise Stop('package_source_inventory_unavailable') from None
    expected = {}
    for name in names:
        path = Path(name)
        if path.parent == Path('aws/shared') and path.suffix == '.py':
            expected[path.name] = name
    for name in names:
        path = Path(name)
        if path.parent == source.parent.relative_to(ROOT) and path.suffix == '.py':
            expected[path.name] = name
    require('lambda_function.py' in expected and len(expected) <= 1000,
            'package_source_inventory_unqualified')
    with zipfile.ZipFile(io.BytesIO(raw)) as archive:
        require(len(archive.namelist()) <= 2000, 'package_member_count_bound')
        for name, repo_name in expected.items():
            require(archive.namelist().count(name) == 1 and archive.getinfo(name).file_size <= SOURCE_BOUND,
                    'package_source_member_unqualified')
            require(archive.read(name) == commit_source(receipt_commit, ROOT / repo_name),
                    'package_repository_source_mismatch')
        require({name for name in archive.namelist() if name.endswith('.py')} == set(expected),
                'package_extra_python_source')
    return {'repository_python_members_verified': len(expected), 'receipt_commit': receipt_commit,
            'signed_ZIP_binds_all_package_bytes': True, 'bytecode_reconstruction_qualified': False}


def inspect(lam, s3, events, scheduler, iam, reader, opener=urllib.request.urlopen):
    require(not BASELINE.exists(), 'prospective_baseline_already_exists_no_recapture')
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
    require(code_sha256 == live.get('CodeSha256') and type(live.get('CodeSize')) is int
            and live['CodeSize'] == len(raw), 'package_hash_mismatch')
    with zipfile.ZipFile(io.BytesIO(raw)) as archive:
        require(archive.namelist().count('lambda_function.py') == 1, 'source_member_not_unique')
        require(archive.getinfo('lambda_function.py').file_size <= SOURCE_BOUND, 'source_member_byte_bound')
        actual = archive.read('lambda_function.py')
    require(actual == before == expected, 'installed_handler_not_exact_reviewed_PR75')
    require(code_sha256 == INSTALLED_CODE_SHA256, 'installed_package_changed')
    handler_sha256 = hashlib.sha256(actual).hexdigest()
    runtime = runtime_evidence(live, reader.read(lam.get_runtime_management_config,
        FunctionName=FUNCTION, Qualifier='$LATEST'))
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
    historical_raw = HISTORICAL_BASELINE.read_bytes()
    require(hashlib.sha256(historical_raw).hexdigest() == HISTORICAL_SHA256,
            'historical_baseline_bytes_changed')
    historical = json.loads(historical_raw)
    require(historical.get('operating_fingerprint') == digest(historical.get('operating_controls')),
            'historical_baseline_integrity_failed')
    differences = control_differences(historical['operating_controls'], operating)
    require(all(row['path'] == 'private_configuration.RuntimeVersionConfig' for row in differences),
            'unapproved_historical_control_difference')
    # This is a newly authorized prospective capture. It cannot satisfy or
    # reinterpret the historical PR75 exact-runtime equality check.
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
    require(receipt is not None and receipt_commit == INSTALLED_RELEASE
            and type(receipt.get('run_id')) is str and receipt['run_id'] == INSTALLED_RELEASE_RUN,
            'installed_receipt_not_exact')
    package_sources = package_source_evidence(raw, receipt_commit, source)
    live_after = reader.read(lam.get_function, FunctionName=FUNCTION)['Configuration']
    runtime_after = runtime_evidence(live_after, reader.read(lam.get_runtime_management_config,
        FunctionName=FUNCTION, Qualifier='$LATEST'))
    require(live_after.get('FunctionName') == FUNCTION
            and live_after.get('FunctionArn') == live['FunctionArn']
            and type(live_after.get('CodeSize')) is int and live_after['CodeSize'] == len(raw)
            and technical_controls(live_after, cfg) == technical_controls(live, cfg)
            and runtime_after == runtime and live_after.get('CodeSha256') == code_sha256
            and live_after.get('State') == 'Active' and live_after.get('LastUpdateStatus') == 'Successful',
            'capture_changed_between_observations')
    try:
        simulated = simulation_evidence(iam, reader)
    except Stop as exc:
        # Only optional IAM access/transport refusal may preserve an otherwise
        # qualified capture. Scope, bounds, malformed SDK or programming errors
        # still fail the whole probe. This does not establish LIST authority.
        if type(exc) is not Stop or str(exc) not in ('access_denied_stop', 'aws_read_failed_details_withheld'):
            raise
        simulated = {'modeled_evidence_available': False, 'stop_reason': str(exc),
            'all_identity_simulations_allowed': False,
            'native_execution_role_LIST_proven': False,
            'scope': 'IAM simulation unavailable; exact execution-role LIST permission remains unqualified.'}
    operating['runtime_evidence'] = runtime
    fingerprint = digest(operating)
    return {'source_phase': 'installed_PR75_prospective_predecessor', 'handler_sha256': handler_sha256,
            'code_sha256': code_sha256, 'zip_sha256_hex': hashlib.sha256(raw).hexdigest(),
            'zip_bytes': len(raw), 'receipt_commit': receipt_commit, 'deployed_at': deployed_at,
            'operating_fingerprint': fingerprint, 'operating_controls': operating,
            'baseline_compared': False,
            'schema': 'series-reliability-prospective-baseline.v1',
            'captured_at': datetime.now().astimezone().isoformat(),
            'capture_commit': subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=ROOT, timeout=15).decode().strip(),
            'authorization': 'Separate user decision 2026-10-02T13:58:48Z; fresh prospective PR78/81 baseline; historical PR75 remains failed/unqualified.',
            'approved_release_sources': RELEASE_SOURCES,
            'historical_PR75': {'status': 'failed_not_recovered',
                'baseline_sha256': hashlib.sha256(HISTORICAL_BASELINE.read_bytes()).hexdigest(),
                'failure_report_commit': '6b97a1b8ff9ba74244b8232a0444e9d935faa3d1',
                'old_runtime_identity_mode_and_cause': 'unavailable'},
            'package_sources': package_sources, 'LIST_permission': simulated,
            'release_qualified': False, 'named_bindings_checked': len(operating['bindings']),
            'aws_read_calls': reader.calls, 'signed_package_gets': 1, 'aws_writes': 0, 'producer_invokes': 0,
            'scope': 'New prospective exact installed package/receipt/repository Python source capture, runtime ARN/shape and separate management mode, and six named bindings. No atomic cross-call snapshot, live execution-role LIST or natural output qualification. Private environment/security/target payloads remain digested.'}


def main():
    require(os.environ.get('GITHUB_ACTIONS') == 'true', 'runner_only')
    import boto3
    from botocore.config import Config
    from botocore.exceptions import (ClientError, EndpointConnectionError,
        ConnectionClosedError, ConnectTimeoutError, ReadTimeoutError)
    sys.path.insert(0, str(ROOT / 'aws/ops'))
    from ops_report import report
    reader = Reader(ClientError, (EndpointConnectionError, ConnectionClosedError,
                                 ConnectTimeoutError, ReadTimeoutError))

    def deadline(*_):
        raise Stop('time_bound_reached')
    signal.signal(signal.SIGALRM, deadline)
    signal.alarm(120)
    with report(Path(__file__).stem) as out:
        try:
            cfg = Config(connect_timeout=3, read_timeout=8, retries={'total_max_attempts': 1})
            clients = [boto3.client(service, region_name='us-east-1', config=cfg)
                       for service in ('lambda', 's3', 'events', 'scheduler', 'iam')]
            result = inspect(*clients, reader)
            out.log('TECHNICAL_JSON ' + json.dumps(result, sort_keys=True, default=str))
            out.kv(completed=True, source_phase=result['source_phase'], aws_read_calls=reader.calls,
                   aws_writes=0, producer_invokes=0)
        except Stop as exc:
            if isinstance(exc, RuntimeUnqualified):
                out.log('RUNTIME_UNQUALIFIED_JSON ' + json.dumps(exc.diagnostic, sort_keys=True))
            out.kv(completed=False, aws_read_calls=reader.calls, stop_reason=str(exc))
            raise SystemExit(1) from None
        except Exception:
            out.kv(completed=False, aws_read_calls=reader.calls, stop_reason='unexpected_failure_details_withheld')
            raise SystemExit(1) from None
        finally:
            signal.alarm(0)


if __name__ == '__main__':
    main()
